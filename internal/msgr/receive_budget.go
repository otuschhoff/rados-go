package msgr

import (
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
)

var ErrReceiveBudgetExceeded = errors.Join(errors.New("messenger receive budget exceeded"), ErrQueueSaturated)

const receiveControlBytesPerSession = 3 * securePreamble

// ReceiveBudget bounds admitted sessions and internal receive backing storage.
// Reader buffers, metadata, and application-owned messages are separate resources.
// Fixed controls use a separate maxSessions * 3 * 96-byte allowance for the
// active decoder, read-ahead frame, and owner-held frame.
// Custom transports/codecs can allocate before returning; only built-in codecs
// guarantee reservation before payload allocation.
type ReceiveBudget struct {
	mu              sync.Mutex
	maxSessions     int
	maxBytes        uint64
	sessions        int
	bytes           uint64
	controlBytes    uint64
	scratchSlots    atomic.Int32
	scratchMetrics  atomic.Bool
	scratchHits     atomic.Uint64
	scratchMisses   atomic.Uint64
	scratchBypasses atomic.Uint64
}

type ReceiveBudgetSnapshot struct {
	Sessions        int
	RetainedBytes   uint64
	ControlBytes    uint64
	ScratchHits     uint64
	ScratchMisses   uint64
	ScratchBypasses uint64
}

func NewReceiveBudget(maxSessions int, maxBytes uint64) (*ReceiveBudget, error) {
	if maxSessions <= 0 || uint64(maxSessions) > ^uint64(0)/receiveControlBytesPerSession || maxBytes == 0 {
		return nil, fmt.Errorf("%w: limits must be positive", ErrReceiveBudgetExceeded)
	}
	budget := &ReceiveBudget{maxSessions: maxSessions, maxBytes: maxBytes}
	budget.scratchSlots.Store(4)
	return budget, nil
}

func (budget *ReceiveBudget) ConfigureScratchDiagnostic(slots int) error {
	if slots < 1 || slots > 8 {
		return ErrReceiveBudgetExceeded
	}
	budget.mu.Lock()
	defer budget.mu.Unlock()
	if budget.sessions != 0 || budget.bytes != 0 {
		return ErrReceiveBudgetExceeded
	}
	budget.scratchSlots.Store(int32(slots))
	budget.scratchMetrics.Store(true)
	return nil
}

func (budget *ReceiveBudget) recordScratch(hit, bypass bool) {
	if !budget.scratchMetrics.Load() {
		return
	}
	if bypass {
		budget.scratchBypasses.Add(1)
	} else if hit {
		budget.scratchHits.Add(1)
	} else {
		budget.scratchMisses.Add(1)
	}
}

func (budget *ReceiveBudget) Snapshot() ReceiveBudgetSnapshot {
	budget.mu.Lock()
	defer budget.mu.Unlock()
	return ReceiveBudgetSnapshot{Sessions: budget.sessions, RetainedBytes: budget.bytes, ControlBytes: budget.controlBytes, ScratchHits: budget.scratchHits.Load(), ScratchMisses: budget.scratchMisses.Load(), ScratchBypasses: budget.scratchBypasses.Load()}
}

func (budget *ReceiveBudget) admit() error {
	if budget == nil {
		return nil
	}
	budget.mu.Lock()
	defer budget.mu.Unlock()
	if budget.sessions >= budget.maxSessions {
		return ErrReceiveBudgetExceeded
	}
	budget.sessions++
	return nil
}

func (budget *ReceiveBudget) retire() {
	if budget == nil {
		return
	}
	budget.mu.Lock()
	budget.sessions--
	budget.mu.Unlock()
}

type receiveLease struct {
	budget       *ReceiveBudget
	bytes        uint64
	controlBytes uint64
	extra        uint64
	backing      []byte
	scratch      *receiveScratch
}

func (lease *receiveLease) reclaim() {
	if lease != nil && lease.scratch != nil {
		lease.scratch.put(lease)
		return
	}
	lease.release()
}

func (lease *receiveLease) resize(bytes uint64) error {
	if lease == nil {
		return nil
	}
	if bytes > ^uint64(0)-lease.extra {
		return ErrReceiveBudgetExceeded
	}
	bytes += lease.extra
	budget := lease.budget
	budget.mu.Lock()
	defer budget.mu.Unlock()
	if bytes > lease.bytes && bytes-lease.bytes > budget.maxBytes-budget.bytes {
		return ErrReceiveBudgetExceeded
	}
	budget.bytes = budget.bytes - lease.bytes + bytes
	lease.bytes = bytes
	return nil
}

func (lease *receiveLease) releaseControl() {
	if lease == nil {
		return
	}
	lease.budget.mu.Lock()
	lease.budget.controlBytes -= lease.controlBytes
	lease.controlBytes = 0
	lease.budget.mu.Unlock()
}

func (lease *receiveLease) release() {
	if lease == nil {
		return
	}
	budget := lease.budget
	budget.mu.Lock()
	budget.bytes -= lease.bytes
	budget.controlBytes -= lease.controlBytes
	lease.bytes, lease.controlBytes = 0, 0
	lease.backing, lease.scratch, lease.extra = nil, nil, 0
	budget.mu.Unlock()
}

type budgetReader struct {
	io.Reader
	lease   *receiveLease
	prelude []byte
	scratch *receiveScratch
}

type budgetReceiveReservation struct {
	reader budgetReader
	lease  receiveLease
}

type budgetSecureReservation struct {
	budgetReceiveReservation
	prelude [securePreamble]byte
}

func (reader *budgetReader) receivePreludeBuffer() []byte { return reader.prelude }

func (reader budgetReader) reserveReceiveBytes(bytes uint64) error {
	err := reader.lease.resize(bytes)
	if err != nil && reader.scratch != nil {
		reader.scratch.dropIdle()
		err = reader.lease.resize(bytes)
	}
	return err
}

func (reader *budgetReader) receiveBacking(size int) ([]byte, error) {
	if reader.scratch == nil {
		return make([]byte, size), nil
	}
	return reader.scratch.get(reader.lease, size)
}

func (reader budgetReader) releaseReceivePrelude() { reader.lease.releaseControl() }

func (reader budgetReader) reserveReceivePrelude() error {
	budget := reader.lease.budget
	budget.mu.Lock()
	defer budget.mu.Unlock()
	if uint64(securePreamble) > uint64(budget.maxSessions)*receiveControlBytesPerSession-budget.controlBytes {
		return ErrReceiveBudgetExceeded
	}
	budget.controlBytes += securePreamble
	reader.lease.controlBytes = securePreamble
	return nil
}

func reserveReceivePrelude(reader io.Reader) error {
	if reservation, ok := reader.(interface{ reserveReceivePrelude() error }); ok {
		return reservation.reserveReceivePrelude()
	}
	return nil
}

func releaseReceivePrelude(reader io.Reader) {
	if reservation, ok := reader.(interface{ releaseReceivePrelude() }); ok {
		reservation.releaseReceivePrelude()
	}
}

func reserveReceiveBytes(reader io.Reader, bytes uint64) error {
	if reservation, ok := reader.(interface{ reserveReceiveBytes(uint64) error }); ok {
		return reservation.reserveReceiveBytes(bytes)
	}
	return nil
}

// BudgetReadTransport forwards preallocation-aware reads through auth wrappers.
type BudgetReadTransport interface {
	ReadFrameWithBudget(*ReceiveBudget, Limits) (Frame, error)
}

// ReadTransportFrame detaches custom transport storage after validating its size.
// It cannot bound allocations made inside a custom ReadFrame implementation.
func ReadTransportFrame(transport Transport, budget *ReceiveBudget, limits Limits) (Frame, error) {
	if reader, ok := transport.(BudgetReadTransport); ok {
		frame, err := reader.ReadFrameWithBudget(budget, limits)
		if err == nil {
			err = validateReceiveFrame(frame, limits)
		}
		if err != nil {
			frame.receiveLease.release()
			return Frame{}, err
		}
		if budget != nil && frame.receiveLease == nil {
			return detachReceiveFrame(frame, budget, limits)
		}
		return frame, nil
	}
	frame, err := transport.ReadFrame()
	if err != nil {
		frame.receiveLease.release()
		return Frame{}, err
	}
	return detachReceiveFrame(frame, budget, limits)
}

func detachReceiveFrame(frame Frame, budget *ReceiveBudget, limits Limits) (Frame, error) {
	if err := validateReceiveFrame(frame, limits); err != nil {
		return Frame{}, err
	}
	var bytes uint64
	for _, segment := range frame.Segments {
		bytes += uint64(len(segment.Data))
	}
	var lease *receiveLease
	if budget != nil {
		lease = &receiveLease{budget: budget}
		if err := lease.resize(bytes); err != nil {
			return Frame{}, err
		}
	}
	segments := make([]Segment, len(frame.Segments))
	for index, segment := range frame.Segments {
		data := make([]byte, len(segment.Data))
		copy(data, segment.Data)
		segments[index] = Segment{Alignment: segment.Alignment, Data: data}
	}
	frame.Segments, frame.receiveLease = segments, lease
	return frame, nil
}

func validateReceiveFrame(frame Frame, limits Limits) error {
	if !validTag(frame.Tag) || len(frame.Segments) < 1 || len(frame.Segments) > MaxSegments {
		return ErrMalformed
	}
	bytes := uint64(PreambleSize)
	if len(frame.Segments[0].Data) > 0 {
		bytes += 4
	}
	if len(frame.Segments) > 1 {
		bytes += 13
	}
	for _, segment := range frame.Segments {
		length := uint64(len(segment.Data))
		if !validAlignment(segment.Alignment) {
			return ErrMalformed
		}
		if length > uint64(limits.MaxSegmentBytes) || bytes > limits.MaxFrameBytes || length > limits.MaxFrameBytes-bytes {
			return ErrLimitExceeded
		}
		bytes += length
	}
	return nil
}
