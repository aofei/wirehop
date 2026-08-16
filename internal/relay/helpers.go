package relay

import (
	"context"
	"errors"
	"fmt"

	"github.com/aofei/wirehop/internal/carrier"
	"github.com/aofei/wirehop/internal/clockmap"
	"github.com/aofei/wirehop/internal/protocol"
)

// IsProtocolViolation reports deterministic peer input that reconnecting the same implementation cannot repair.
func IsProtocolViolation(err error) bool {
	return errors.Is(err, protocol.ErrInvalidMagic) || errors.Is(err, protocol.ErrUnsupportedVersion) ||
		errors.Is(err, carrier.ErrInvalidWebSocketMessage) ||
		errors.Is(err, protocol.ErrInvalidServerHello) || errors.Is(err, protocol.ErrInvalidFrameType) ||
		errors.Is(err, protocol.ErrFrameTooLarge) || errors.Is(err, protocol.ErrTrailingFrameData) ||
		errors.Is(err, protocol.ErrInvalidDataFrame) ||
		errors.Is(err, protocol.ErrInvalidControlFrame) || errors.Is(err, protocol.ErrProbeTooLarge) ||
		errors.Is(err, clockmap.ErrInvalidSample) || errors.Is(err, clockmap.ErrTimestampOverflow) ||
		errors.Is(err, ErrInvalidWireGuardPacket) || errors.Is(err, ErrInvalidPacketDeadline) ||
		errors.Is(err, ErrInvalidDeliveryReport) || errors.Is(err, ErrUnexpectedPong) ||
		errors.Is(err, ErrClockSyncRequired) || errors.Is(err, ErrUnexpectedFrame)
}

// queueResult normalizes queue closure caused by cancellation.
func queueResult(ctx context.Context, operation string, err error) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return fmt.Errorf("%s: %w", operation, err)
}
