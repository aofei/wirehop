package protocol

// MaxMeasurementHoldMicros bounds a measurement's remaining Data pause independently of liveness timeouts.
const MaxMeasurementHoldMicros = 4_000_000

// MaxMeasurementTargets guarantees that even full-width identities fit the existing absolute frame limit.
const MaxMeasurementTargets = (MaxFrameContentSize - 14) / 20

// MeasurementKind identifies a phase of the client-coordinated measurement transaction.
type MeasurementKind uint8

const (
	// MeasurementRequest asks the client to consider a measurement without promising a pause.
	MeasurementRequest MeasurementKind = iota + 1
	// MeasurementBegin requests parallel preparation of the session Data pause.
	MeasurementBegin
	// MeasurementReady confirms that the server has drained its committed Data prefixes.
	MeasurementReady
	// MeasurementProbe starts client timing after local proof and server readiness.
	MeasurementProbe
	// MeasurementReply returns server timestamps and a remaining pause promise. A zero hold rejects the probe.
	MeasurementReply
	// MeasurementResult completes the transaction with the client's reply-receive timestamp or a rejection.
	MeasurementResult
)

// Measurement carries one phase. Only the fields encoded for that phase may be nonzero.
// ProbeMicros, ReceiveMicros, and ReplyMicros are t1, t2, and t3 in a Reply. ReceiveMicros is t4 in a Result.
type Measurement struct {
	Kind          MeasurementKind
	Targets       []LaneGeneration
	ID            uint64
	ProbeMicros   uint64
	ReceiveMicros uint64
	ReplyMicros   uint64
	HoldMicros    uint64
	Valid         bool
}

// MarshalMeasurement returns one canonical measurement frame.
func MarshalMeasurement(value Measurement) (Frame, error) {
	if value.Kind < MeasurementRequest || value.Kind > MeasurementResult ||
		(value.Kind == MeasurementRequest) != (value.ID == 0) ||
		value.HoldMicros > MaxMeasurementHoldMicros || value.Kind != MeasurementBegin && len(value.Targets) != 0 {
		return Frame{}, ErrInvalidControlFrame
	}
	var payload []byte
	switch value.Kind {
	case MeasurementRequest, MeasurementBegin:
		if value.ProbeMicros != 0 || value.ReceiveMicros != 0 || value.ReplyMicros != 0 || value.HoldMicros != 0 || value.Valid {
			return Frame{}, ErrInvalidControlFrame
		}
		if value.Kind == MeasurementRequest {
			payload = integerPayload(0, uint64(value.Kind))
		} else {
			if len(value.Targets) == 0 || len(value.Targets) > MaxMeasurementTargets {
				return Frame{}, ErrInvalidControlFrame
			}
			values := make([]uint64, 0, 3+2*len(value.Targets))
			values = append(values, uint64(value.Kind), value.ID, uint64(len(value.Targets)))
			var previous LaneID
			for _, target := range value.Targets {
				if target.LaneID <= previous || target.Generation == 0 {
					return Frame{}, ErrInvalidControlFrame
				}
				previous = target.LaneID
				values = append(values, uint64(target.LaneID), target.Generation)
			}
			payload = integerPayload(0, values...)
		}
	case MeasurementReady:
		if value.ProbeMicros != 0 || value.ReceiveMicros != 0 || value.ReplyMicros != 0 || value.HoldMicros != 0 {
			return Frame{}, ErrInvalidControlFrame
		}
		var valid uint64
		if value.Valid {
			valid = 1
		}
		payload = integerPayload(0, uint64(value.Kind), value.ID, valid)
	case MeasurementProbe:
		if value.ReceiveMicros != 0 || value.ReplyMicros != 0 || value.HoldMicros == 0 || value.Valid {
			return Frame{}, ErrInvalidControlFrame
		}
		payload = integerPayload(0, uint64(value.Kind), value.ID, value.ProbeMicros, value.HoldMicros)
	case MeasurementReply:
		if value.ReplyMicros < value.ReceiveMicros || value.Valid {
			return Frame{}, ErrInvalidControlFrame
		}
		payload = integerPayload(0, uint64(value.Kind), value.ID, value.ProbeMicros,
			value.ReceiveMicros, value.ReplyMicros, value.HoldMicros)
	case MeasurementResult:
		if value.ProbeMicros != 0 || value.ReplyMicros != 0 || value.HoldMicros != 0 || !value.Valid && value.ReceiveMicros != 0 {
			return Frame{}, ErrInvalidControlFrame
		}
		var valid uint64
		if value.Valid {
			valid = 1
		}
		payload = integerPayload(0, uint64(value.Kind), value.ID, valid, value.ReceiveMicros)
	}
	return Frame{Type: FrameMeasurement, Payload: payload}, nil
}

// ParseMeasurement parses exactly the fields of one phase and rejects noncanonical or unbounded values.
func ParseMeasurement(frame Frame) (Measurement, error) {
	if frame.Type != FrameMeasurement {
		return Measurement{}, ErrInvalidControlFrame
	}
	var kind uint64
	remaining, err := parseIntegers(frame.Payload, &kind)
	if err != nil || kind < uint64(MeasurementRequest) || kind > uint64(MeasurementResult) {
		return Measurement{}, ErrInvalidControlFrame
	}
	value := Measurement{Kind: MeasurementKind(kind)}
	var valid uint64
	switch value.Kind {
	case MeasurementRequest:
	case MeasurementBegin:
		var count uint64
		remaining, err = parseIntegers(remaining, &value.ID, &count)
		if err != nil || count == 0 || count > MaxMeasurementTargets || count > uint64(len(remaining)/2) {
			return Measurement{}, ErrInvalidControlFrame
		}
		value.Targets = make([]LaneGeneration, count)
		var previous LaneID
		for index := range value.Targets {
			target := &value.Targets[index]
			remaining, err = parseIntegers(remaining, (*uint64)(&target.LaneID), &target.Generation)
			if err != nil || target.LaneID <= previous || target.Generation == 0 {
				return Measurement{}, ErrInvalidControlFrame
			}
			previous = target.LaneID
		}
	case MeasurementReady:
		remaining, err = parseIntegers(remaining, &value.ID, &valid)
		value.Valid = valid == 1
	case MeasurementProbe:
		remaining, err = parseIntegers(remaining, &value.ID, &value.ProbeMicros, &value.HoldMicros)
	case MeasurementReply:
		remaining, err = parseIntegers(remaining, &value.ID, &value.ProbeMicros,
			&value.ReceiveMicros, &value.ReplyMicros, &value.HoldMicros)
	case MeasurementResult:
		remaining, err = parseIntegers(remaining, &value.ID, &valid, &value.ReceiveMicros)
		value.Valid = valid == 1
	}
	if err != nil || len(remaining) != 0 || valid > 1 ||
		(value.Kind == MeasurementRequest) != (value.ID == 0) || value.HoldMicros > MaxMeasurementHoldMicros ||
		value.Kind == MeasurementProbe && value.HoldMicros == 0 ||
		value.Kind == MeasurementReply && value.ReplyMicros < value.ReceiveMicros ||
		value.Kind == MeasurementResult && !value.Valid && value.ReceiveMicros != 0 {
		return Measurement{}, ErrInvalidControlFrame
	}
	return value, nil
}
