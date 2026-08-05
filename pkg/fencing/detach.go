package fencing

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	ec2types "github.com/aws/aws-sdk-go-v2/service/ec2/types"
)

// VolumeDetacher severs a fenced node's access to the shared block device at
// the infrastructure layer, and confirms the severance actually took effect.
//
// This is the half of fencing that etcd cannot provide.  A generation bump
// stops a fenced node from *publishing* anything, but it does not stop the
// node's kernel from continuing to issue writes to the raw device — EBS
// Multi-Attach has no equivalent of SCSI-3 persistent reservations, so
// nothing at the storage layer will reject them.  Detaching the volume is the
// only mechanism that makes those writes physically impossible rather than
// merely unreachable.
//
// Implementations must be safe to call on a node that is already detached:
// fencing is retried, and a node can expire more than once.
type VolumeDetacher interface {
	// DetachAndConfirm detaches the shared volume from instanceID and blocks
	// until the detachment is confirmed by a separate read of the volume's
	// state, or ctx expires.  Returning nil means the instance is confirmed
	// to no longer hold an attachment.
	DetachAndConfirm(ctx context.Context, instanceID string) error
}

// ec2API is the slice of the EC2 client this package uses, extracted so tests
// can substitute a fake without reaching for the network.
type ec2API interface {
	DetachVolume(ctx context.Context, in *ec2.DetachVolumeInput, opts ...func(*ec2.Options)) (*ec2.DetachVolumeOutput, error)
	DescribeVolumes(ctx context.Context, in *ec2.DescribeVolumesInput, opts ...func(*ec2.Options)) (*ec2.DescribeVolumesOutput, error)
}

// EBSDetacher detaches an EBS Multi-Attach volume via the EC2 API.
type EBSDetacher struct {
	api      ec2API
	volumeID string

	// PollInterval and PollTimeout bound the confirmation phase.  Detachment
	// is asynchronous: DetachVolume returns as soon as the request is
	// accepted, not when the device is actually gone, so the returned state
	// must be polled.  AWS documents no hard upper bound on how long residual
	// I/O may continue, which is precisely why the confirmation exists and
	// why a timeout here is reported as a failure to fence rather than
	// silently treated as success.
	PollInterval time.Duration
	PollTimeout  time.Duration
}

// NewEBSDetacher builds a detacher against the caller's default AWS config
// (environment, shared config, or instance role).
func NewEBSDetacher(ctx context.Context, volumeID string) (*EBSDetacher, error) {
	if volumeID == "" {
		return nil, fmt.Errorf("ebs detacher: volume ID is required")
	}
	// WithEC2IMDSRegion is required, not automatic: the SDK's default region
	// resolvers check AWS_REGION / the shared config file / an explicit
	// WithRegion call, but do NOT query the instance metadata service unless
	// this option opts in — confirmed by reading
	// aws-sdk-go-v2/config@.../load_options.go's getEC2IMDSRegion, which
	// returns not-found whenever UseEC2IMDSRegion is nil. Without this, a bare
	// EC2 instance with no AWS_REGION set (the case here — nodes are
	// provisioned without one) fails with "Missing Region" on every AWS call,
	// which is exactly what happened on the first real run of this code
	// (see docs/chaos-reports/2026-08-05-external-fencing-detach.md).
	cfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithEC2IMDSRegion())
	if err != nil {
		return nil, fmt.Errorf("ebs detacher: load aws config: %w", err)
	}
	return &EBSDetacher{
		api:          ec2.NewFromConfig(cfg),
		volumeID:     volumeID,
		PollInterval: 2 * time.Second,
		PollTimeout:  60 * time.Second,
	}, nil
}

// DetachAndConfirm implements VolumeDetacher.
//
// The two steps are deliberately distinct, and the second is the one that
// matters.  DetachVolume merely *requests* the detachment; treating its
// success as proof the node has stopped writing is the exact mistake this
// method exists to avoid.  Only the subsequent DescribeVolumes read, showing
// the instance absent from the volume's attachment list, is evidence.
func (d *EBSDetacher) DetachAndConfirm(ctx context.Context, instanceID string) error {
	if instanceID == "" {
		return fmt.Errorf("detach %s: instance ID is empty", d.volumeID)
	}

	// Force is required: a graceful detach asks the guest OS to unmount
	// first, which a wedged or partitioned node will never do — and those are
	// exactly the nodes worth fencing.
	_, err := d.api.DetachVolume(ctx, &ec2.DetachVolumeInput{
		VolumeId:   aws.String(d.volumeID),
		InstanceId: aws.String(instanceID),
		Force:      aws.Bool(true),
	})
	if err != nil && !alreadyDetached(err) {
		return fmt.Errorf("detach %s from %s: %w", d.volumeID, instanceID, err)
	}

	deadline := time.Now().Add(d.PollTimeout)
	for {
		attached, aerr := d.stillAttached(ctx, instanceID)
		if aerr != nil {
			return fmt.Errorf("confirm detach %s from %s: %w", d.volumeID, instanceID, aerr)
		}
		if !attached {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("confirm detach %s from %s: still attached after %s",
				d.volumeID, instanceID, d.PollTimeout)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(d.PollInterval):
		}
	}
}

// stillAttached reports whether instanceID still holds an attachment to the
// volume.  A "detaching" attachment counts as still attached — the whole
// point of the confirmation is to wait for it to finish.
func (d *EBSDetacher) stillAttached(ctx context.Context, instanceID string) (bool, error) {
	out, err := d.api.DescribeVolumes(ctx, &ec2.DescribeVolumesInput{
		VolumeIds: []string{d.volumeID},
	})
	if err != nil {
		return false, err
	}
	for _, vol := range out.Volumes {
		for _, att := range vol.Attachments {
			if att.InstanceId == nil || *att.InstanceId != instanceID {
				continue
			}
			if att.State == ec2types.VolumeAttachmentStateDetached {
				continue
			}
			return true, nil
		}
	}
	return false, nil
}

// alreadyDetached reports whether an error means the work is already done.
// Fencing is retried and a node can expire more than once, so "there was no
// attachment to remove" is a success, not a failure.
func alreadyDetached(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	for _, s := range []string{"IncorrectState", "InvalidAttachment.NotFound", "is not attached"} {
		if strings.Contains(msg, s) {
			return true
		}
	}
	return false
}
