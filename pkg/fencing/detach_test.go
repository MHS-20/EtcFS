package fencing

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	ec2types "github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeEC2 models the part of EBS behaviour that matters here: DetachVolume is
// a *request*, not the detachment itself, so the attachment stays visible for
// a while afterwards.  detachAfter says how many DescribeVolumes calls must
// elapse before the attachment disappears.
type fakeEC2 struct {
	detachCalls  int
	describes    int
	detachAfter  int
	detachErr    error
	describeErr  error
	attachedTo   string
	lastForce    bool
	lastInstance string
}

func (f *fakeEC2) DetachVolume(_ context.Context, in *ec2.DetachVolumeInput, _ ...func(*ec2.Options)) (*ec2.DetachVolumeOutput, error) {
	f.detachCalls++
	if in.InstanceId != nil {
		f.lastInstance = *in.InstanceId
	}
	if in.Force != nil {
		f.lastForce = *in.Force
	}
	if f.detachErr != nil {
		return nil, f.detachErr
	}
	return &ec2.DetachVolumeOutput{}, nil
}

func (f *fakeEC2) DescribeVolumes(_ context.Context, _ *ec2.DescribeVolumesInput, _ ...func(*ec2.Options)) (*ec2.DescribeVolumesOutput, error) {
	f.describes++
	if f.describeErr != nil {
		return nil, f.describeErr
	}
	if f.describes > f.detachAfter {
		return &ec2.DescribeVolumesOutput{Volumes: []ec2types.Volume{{}}}, nil
	}
	return &ec2.DescribeVolumesOutput{Volumes: []ec2types.Volume{{
		Attachments: []ec2types.VolumeAttachment{{
			InstanceId: aws.String(f.attachedTo),
			State:      ec2types.VolumeAttachmentStateAttached,
		}},
	}}}, nil
}

func newTestDetacher(f *fakeEC2) *EBSDetacher {
	return &EBSDetacher{
		api:          f,
		volumeID:     "vol-test",
		PollInterval: time.Millisecond,
		PollTimeout:  time.Second,
	}
}

func TestDetach_ConfirmsBeforeReturning(t *testing.T) {
	f := &fakeEC2{attachedTo: "i-123", detachAfter: 3}
	d := newTestDetacher(f)

	require.NoError(t, d.DetachAndConfirm(context.Background(), "i-123"))

	assert.Equal(t, 1, f.detachCalls, "should request detach once")
	assert.Greater(t, f.describes, 1,
		"must poll until the attachment is actually gone, not trust DetachVolume's return")
	assert.True(t, f.lastForce, "force is required — a wedged node will never unmount cooperatively")
	assert.Equal(t, "i-123", f.lastInstance)
}

// The failure that matters: the volume is requested detached but never
// actually detaches.  Reporting success here would let the caller bump the
// generation and hand the node's arenas to a peer while it is still writing.
func TestDetach_TimesOutIfStillAttached(t *testing.T) {
	f := &fakeEC2{attachedTo: "i-123", detachAfter: 1 << 30}
	d := newTestDetacher(f)
	d.PollTimeout = 20 * time.Millisecond

	err := d.DetachAndConfirm(context.Background(), "i-123")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "still attached")
}

// A detaching-but-not-yet-detached attachment is not good enough.
func TestDetach_DetachingStateCountsAsAttached(t *testing.T) {
	f := &fakeEC2{attachedTo: "i-123", detachAfter: 1 << 30}
	d := newTestDetacher(f)
	d.PollTimeout = 20 * time.Millisecond

	attached, err := d.stillAttached(context.Background(), "i-123")
	require.NoError(t, err)
	assert.True(t, attached)
}

// Fencing is retried and a node can expire more than once, so an already-gone
// attachment is success rather than an error to escalate.
func TestDetach_AlreadyDetachedIsSuccess(t *testing.T) {
	f := &fakeEC2{
		attachedTo:  "i-123",
		detachAfter: 0, // already gone on the first describe
		detachErr:   errors.New("IncorrectState: Volume vol-test is not attached to instance i-123"),
	}
	d := newTestDetacher(f)

	assert.NoError(t, d.DetachAndConfirm(context.Background(), "i-123"))
}

func TestDetach_OtherInstancesAttachmentsIgnored(t *testing.T) {
	// Multi-Attach: peers legitimately hold their own attachments, and
	// fencing one node must not wait on, or be confused by, the others.
	f := &fakeEC2{attachedTo: "i-other", detachAfter: 1 << 30}
	d := newTestDetacher(f)

	attached, err := d.stillAttached(context.Background(), "i-123")
	require.NoError(t, err)
	assert.False(t, attached, "another instance's attachment is not ours")
}

func TestDetach_EmptyInstanceIDRejected(t *testing.T) {
	d := newTestDetacher(&fakeEC2{})
	err := d.DetachAndConfirm(context.Background(), "")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "instance ID is empty")
}

func TestDetach_DescribeErrorIsNotSuccess(t *testing.T) {
	f := &fakeEC2{attachedTo: "i-123", describeErr: errors.New("AuthFailure")}
	d := newTestDetacher(f)

	err := d.DetachAndConfirm(context.Background(), "i-123")
	require.Error(t, err, "an unreadable volume state is not proof of detachment")
	assert.Contains(t, err.Error(), "confirm detach")
}
