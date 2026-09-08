//go:build linux

package datagram

import (
	"encoding/binary"
	"errors"
	"net"
	"net/netip"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"unsafe"

	"github.com/aofei/wirehop/internal/protocol"
	"golang.org/x/sys/unix"
)

const (
	// maximumConcurrentReadBatches bounds process-wide opportunistic staging memory.
	maximumConcurrentReadBatches = 16
	// readBatchDrainSize leaves the first blocking read outside the opportunistic drain.
	readBatchDrainSize = MaximumBatchSize - 1
	// udpSegmentSizeBytes is the UDP_SEGMENT ancillary value size.
	udpSegmentSizeBytes = 2
	// udpSegmentControlCapacity holds one aligned UDP_SEGMENT control message on supported Linux architectures.
	udpSegmentControlCapacity = unix.SizeofCmsghdr + 8
	// maximumIPv4UDPPayload accounts for the IPv4 and UDP headers.
	maximumIPv4UDPPayload = 1<<16 - 1 - 20 - 8
	// maximumIPv6UDPPayload accounts for the UDP header because the IPv6 length excludes its own header.
	maximumIPv6UDPPayload = 1<<16 - 1 - 8
	// minimumEqualTailWorkaroundBatchSize avoids disproportionate sentinel traffic for small batches.
	minimumEqualTailWorkaroundBatchSize = 8
	// maximumWriteVectorSize includes one sentinel for every possible two-packet GSO run.
	maximumWriteVectorSize = MaximumBatchSize + MaximumBatchSize/2
)

var (
	// invalidWireGuardDatagram terminates affected equal-size GSO runs with a harmless shorter segment.
	invalidWireGuardDatagram = [1]byte{}
	// linuxEqualGSOTailWorkaround records whether the running kernel needs the UDP GSO workaround.
	linuxEqualGSOTailWorkaround = currentKernelHasEqualGSOTailBug()
	// linuxReadBatches bounds borrowed read vectors while retaining them for reuse.
	linuxReadBatches = newLinuxReadBatchPool()
	// linuxWriteBatches reuses sendmmsg metadata without retaining payloads.
	linuxWriteBatches = sync.Pool{New: func() any { return newLinuxWriteBatch() }}
)

// linuxMMsgHdr matches Linux struct mmsghdr on every supported architecture.
type linuxMMsgHdr struct {
	header unix.Msghdr
	length uint32
}

// linuxUDPBatchConn applies Linux mmsg operations to one UDP socket.
type linuxUDPBatchConn struct {
	raw                    syscall.RawConn
	gso                    bool
	equalGSOTailWorkaround bool
}

// linuxReadBatch contains one bounded nonblocking receive vector.
type linuxReadBatch struct {
	headers   [readBatchDrainSize]linuxMMsgHdr
	vectors   [readBatchDrainSize]unix.Iovec
	addresses [readBatchDrainSize]unix.RawSockaddrAny
	buffers   [readBatchDrainSize][protocol.MaxPacketSize + 1]byte
	values    [readBatchDrainSize]udpMessage
	limit     int
	count     int
	err       syscall.Errno
	receive   func(uintptr) bool
}

// linuxReadBatchPool limits concurrent large receive vectors process-wide.
type linuxReadBatchPool struct {
	permits chan struct{}
	idle    chan *linuxReadBatch
}

// linuxWriteBatch contains reusable sendmmsg metadata and socket addresses.
type linuxWriteBatch struct {
	headers      [MaximumBatchSize]linuxMMsgHdr
	vectors      [maximumWriteVectorSize]unix.Iovec
	addresses    [MaximumBatchSize]unix.RawSockaddrAny
	controls     [MaximumBatchSize][udpSegmentControlCapacity]byte
	segments     [MaximumBatchSize]int
	headersCount int
	vectorsCount int
	offset       int
	limit        int
	count        int
	err          syscall.Errno
	send         func(uintptr) bool
}

// newUDPBatchConn prepares direct mmsg access to conn.
func newUDPBatchConn(conn *net.UDPConn) udpBatchConn {
	raw, err := conn.SyscallConn()
	if err != nil {
		return nil
	}
	gso := false
	raw.Control(func(descriptor uintptr) {
		_, optionErr := unix.GetsockoptInt(int(descriptor), unix.IPPROTO_UDP, unix.UDP_SEGMENT)
		gso = optionErr == nil
	})
	return &linuxUDPBatchConn{
		raw: raw, gso: gso, equalGSOTailWorkaround: linuxEqualGSOTailWorkaround,
	}
}

// currentKernelHasEqualGSOTailBug reports whether the running kernel needs the Linux UDP GSO workaround.
func currentKernelHasEqualGSOTailBug() bool {
	var name unix.Utsname
	return unix.Uname(&name) == nil && linuxKernelHasEqualGSOTailBug(unix.ByteSliceToString(name.Release[:]))
}

// linuxKernelHasEqualGSOTailBug reports whether release identifies Linux 7.0 or a Linux 7.1 release candidate before
// rc5.
func linuxKernelHasEqualGSOTailBug(release string) bool {
	majorText, remainder, ok := strings.Cut(release, ".")
	if !ok {
		return false
	}
	minorText, suffix := splitLeadingDecimal(remainder)
	major, majorErr := strconv.Atoi(majorText)
	minor, minorErr := strconv.Atoi(minorText)
	if majorErr != nil || minorErr != nil || major != 7 {
		return false
	}
	if minor == 0 {
		return true
	}
	if minor != 1 {
		return false
	}
	if strings.HasPrefix(suffix, ".") {
		_, suffix = splitLeadingDecimal(suffix[1:])
	}
	if !strings.HasPrefix(suffix, "-rc") {
		return false
	}
	releaseCandidate, ok := leadingDecimal(suffix[len("-rc"):])
	return ok && releaseCandidate < 5
}

// leadingDecimal parses the leading decimal component of a kernel release field.
func leadingDecimal(value string) (int, bool) {
	leading, _ := splitLeadingDecimal(value)
	if leading == "" {
		return 0, false
	}
	number, err := strconv.Atoi(leading)
	return number, err == nil
}

// splitLeadingDecimal separates one leading decimal component from its suffix.
func splitLeadingDecimal(value string) (string, string) {
	end := 0
	for end < len(value) && value[end] >= '0' && value[end] <= '9' {
		end++
	}
	return value[:end], value[end:]
}

// newLinuxReadBatchPool creates a concurrency bound scaled down on small hosts.
func newLinuxReadBatchPool() *linuxReadBatchPool {
	capacity := min(max(runtime.GOMAXPROCS(0), 2), maximumConcurrentReadBatches)
	return &linuxReadBatchPool{
		permits: make(chan struct{}, capacity),
		idle:    make(chan *linuxReadBatch, capacity),
	}
}

// newLinuxReadBatch initializes stable kernel pointers for one receive vector.
func newLinuxReadBatch() *linuxReadBatch {
	batch := new(linuxReadBatch)
	for index := range batch.headers {
		batch.vectors[index].Base = &batch.buffers[index][0]
		batch.vectors[index].SetLen(len(batch.buffers[index]))
		batch.headers[index].header.Iov = &batch.vectors[index]
		batch.headers[index].header.SetIovlen(1)
		batch.headers[index].header.Name = (*byte)(unsafe.Pointer(&batch.addresses[index]))
	}
	batch.receive = batch.receiveFrom
	return batch
}

// newLinuxWriteBatch initializes one reusable send vector.
func newLinuxWriteBatch() *linuxWriteBatch {
	batch := new(linuxWriteBatch)
	batch.send = batch.sendTo
	return batch
}

// acquire borrows a vector without blocking an active UDP reader.
func (p *linuxReadBatchPool) acquire() *linuxReadBatch {
	select {
	case p.permits <- struct{}{}:
	default:
		return nil
	}
	select {
	case batch := <-p.idle:
		return batch
	default:
		return newLinuxReadBatch()
	}
}

// release clears visible messages and returns batch to the bounded pool.
func (p *linuxReadBatchPool) release(batch *linuxReadBatch) {
	clear(batch.values[:batch.count])
	batch.limit = 0
	batch.count = 0
	batch.err = 0
	p.idle <- batch
	<-p.permits
}

// messages returns the received vector while the batch remains borrowed.
func (b *linuxReadBatch) messages() []udpMessage {
	return b.values[:b.count]
}

// release returns b to the process-wide bounded pool.
func (b *linuxReadBatch) release() {
	linuxReadBatches.release(b)
}

// receiveFrom executes one nonblocking recvmmsg call.
func (b *linuxReadBatch) receiveFrom(descriptor uintptr) bool {
	count, _, err := unix.Syscall6(unix.SYS_RECVMMSG, descriptor, uintptr(unsafe.Pointer(&b.headers[0])),
		uintptr(b.limit), unix.MSG_DONTWAIT, 0, 0)
	b.count = 0
	if err == 0 {
		b.count = int(count)
	}
	b.err = err
	return true
}

// readAvailable drains currently queued datagrams without waiting for another arrival.
func (c *linuxUDPBatchConn) readAvailable(limit int) (udpReadBatch, error) {
	batch := linuxReadBatches.acquire()
	if batch == nil {
		return nil, nil
	}
	batch.limit = min(limit, len(batch.headers))
	for index := range batch.limit {
		header := &batch.headers[index]
		header.header.Namelen = uint32(unsafe.Sizeof(batch.addresses[index]))
		header.header.Flags = 0
		header.length = 0
		batch.vectors[index].SetLen(len(batch.buffers[index]))
	}
	if err := c.raw.Read(batch.receive); err != nil {
		batch.release()
		return nil, err
	}
	runtime.KeepAlive(batch)
	if errors.Is(batch.err, syscall.EAGAIN) || errors.Is(batch.err, syscall.EWOULDBLOCK) {
		batch.release()
		return nil, nil
	}
	if batch.err != 0 {
		err := batch.err
		batch.release()
		return nil, err
	}
	accepted := 0
	for index := range batch.count {
		header := &batch.headers[index]
		if header.length > protocol.MaxPacketSize || header.header.Flags&unix.MSG_TRUNC != 0 {
			continue
		}
		peer, ok := rawSockaddrAddrPort(&batch.addresses[index])
		if !ok {
			continue
		}
		batch.values[accepted] = udpMessage{payload: batch.buffers[index][:header.length], peer: peer}
		accepted++
	}
	batch.count = accepted
	if accepted == 0 {
		batch.release()
		return nil, nil
	}
	return batch, nil
}

// sendTo executes one sendmmsg call and lets RawConn wait for socket writability when necessary.
func (b *linuxWriteBatch) sendTo(descriptor uintptr) bool {
	headers := b.headers[b.offset : b.offset+b.limit]
	count, _, err := unix.Syscall6(unix.SYS_SENDMMSG, descriptor, uintptr(unsafe.Pointer(&headers[0])),
		uintptr(len(headers)), 0, 0, 0)
	b.count = 0
	if err == 0 {
		b.count = int(count)
	}
	b.err = err
	return !errors.Is(err, syscall.EAGAIN) && !errors.Is(err, syscall.EWOULDBLOCK)
}

// write sends an ordered UDP vector and returns its successful prefix.
func (c *linuxUDPBatchConn) write(values []udpMessage) (int, error) {
	batch := linuxWriteBatches.Get().(*linuxWriteBatch)
	defer func() {
		batch.reset()
		linuxWriteBatches.Put(batch)
	}()
	if err := batch.prepare(values, c.gso, c.equalGSOTailWorkaround); err != nil {
		return 0, err
	}
	written := 0
	for batch.offset < batch.headersCount {
		batch.limit = batch.headersCount - batch.offset
		if err := c.raw.Write(batch.send); err != nil {
			return written, err
		}
		runtime.KeepAlive(values)
		if batch.err != 0 {
			segmented := batch.headers[batch.offset].header.Control != nil
			if segmented && (batch.err == syscall.EIO || batch.err == syscall.EMSGSIZE || batch.err == syscall.EINVAL) {
				if batch.err == syscall.EIO {
					c.gso = false
				}
				// GSO cannot fragment an oversized segment. Retry only unsent datagrams without segmentation.
				if err := batch.prepare(values[written:], false, false); err != nil {
					return written, err
				}
				continue
			}
			return written, batch.err
		}
		if batch.count == 0 {
			return written, syscall.EIO
		}
		for index := batch.offset; index < batch.offset+batch.count; index++ {
			written += batch.segments[index]
		}
		batch.offset += batch.count
	}
	return written, nil
}

// reset releases payload pointers and restores b to its reusable empty state.
func (b *linuxWriteBatch) reset() {
	clear(b.vectors[:b.vectorsCount])
	clear(b.headers[:b.headersCount])
	clear(b.segments[:b.headersCount])
	b.headersCount = 0
	b.vectorsCount = 0
	b.offset = 0
	b.limit = 0
	b.count = 0
	b.err = 0
}

// prepare packs values into sendmmsg entries and coalesces compatible runs with UDP_SEGMENT.
func (b *linuxWriteBatch) prepare(values []udpMessage, gso, workAroundEqualTail bool) error {
	b.reset()
	for _, value := range values {
		if len(value.payload) == 0 || !value.peer.IsValid() {
			return syscall.EINVAL
		}
	}
	affectedKernel := workAroundEqualTail
	workAroundEqualTail = gso && affectedKernel && len(values) >= minimumEqualTailWorkaroundBatchSize
	if gso && affectedKernel && len(values) < minimumEqualTailWorkaroundBatchSize {
		gso = false
	}
	for start := 0; start < len(values); {
		end := start + 1
		segmentSize := len(values[start].payload)
		totalSize := segmentSize
		maximumSize := maximumIPv4UDPPayload
		if values[start].peer.Addr().Is6() {
			maximumSize = maximumIPv6UDPPayload
		}
		if workAroundEqualTail {
			maximumSize -= len(invalidWireGuardDatagram)
		}
		if gso {
			for end < len(values) && values[end].peer == values[start].peer {
				size := len(values[end].payload)
				if size > segmentSize || totalSize > maximumSize-size {
					break
				}
				totalSize += size
				end++
				if size < segmentSize {
					break
				}
			}
		}
		headerIndex := b.headersCount
		nameLength, ok := putRawSockaddr(&b.addresses[headerIndex], values[start].peer)
		if !ok {
			return syscall.EINVAL
		}
		vectorStart := b.vectorsCount
		for index := start; index < end; index++ {
			vector := &b.vectors[b.vectorsCount]
			vector.Base = &values[index].payload[0]
			vector.SetLen(len(values[index].payload))
			b.vectorsCount++
		}
		if workAroundEqualTail && end-start > 1 && len(values[end-1].payload) == segmentSize {
			vector := &b.vectors[b.vectorsCount]
			vector.Base = &invalidWireGuardDatagram[0]
			vector.SetLen(len(invalidWireGuardDatagram))
			b.vectorsCount++
		}
		header := &b.headers[headerIndex].header
		header.Iov = &b.vectors[vectorStart]
		header.SetIovlen(b.vectorsCount - vectorStart)
		header.Name = (*byte)(unsafe.Pointer(&b.addresses[headerIndex]))
		header.Namelen = nameLength
		header.Control = nil
		header.SetControllen(0)
		header.Flags = 0
		b.headers[headerIndex].length = 0
		b.segments[headerIndex] = end - start
		if end-start > 1 {
			setUDPSegment(header, &b.controls[headerIndex], uint16(segmentSize))
		}
		b.headersCount++
		start = end
	}
	return nil
}

// setUDPSegment attaches one native-endian UDP_SEGMENT control value.
func setUDPSegment(header *unix.Msghdr, storage *[udpSegmentControlCapacity]byte, segmentSize uint16) {
	control := storage[:unix.CmsgSpace(udpSegmentSizeBytes)]
	clear(control)
	controlHeader := (*unix.Cmsghdr)(unsafe.Pointer(&control[0]))
	controlHeader.Level = unix.IPPROTO_UDP
	controlHeader.Type = unix.UDP_SEGMENT
	controlHeader.SetLen(unix.CmsgLen(udpSegmentSizeBytes))
	binary.NativeEndian.PutUint16(control[unix.CmsgLen(0):], segmentSize)
	header.Control = &control[0]
	header.SetControllen(len(control))
}

// rawSockaddrAddrPort decodes one kernel sockaddr without allocating an address wrapper.
func rawSockaddrAddrPort(address *unix.RawSockaddrAny) (netip.AddrPort, bool) {
	switch address.Addr.Family {
	case unix.AF_INET:
		value := (*unix.RawSockaddrInet4)(unsafe.Pointer(address))
		return netip.AddrPortFrom(netip.AddrFrom4(value.Addr), networkOrderUint16(value.Port)), true
	case unix.AF_INET6:
		value := (*unix.RawSockaddrInet6)(unsafe.Pointer(address))
		ip := netip.AddrFrom16(value.Addr)
		if value.Scope_id != 0 {
			ip = ip.WithZone(interfaceName(value.Scope_id))
		}
		return netip.AddrPortFrom(ip, networkOrderUint16(value.Port)), true
	default:
		return netip.AddrPort{}, false
	}
}

// putRawSockaddr encodes one UDP peer into reusable kernel address storage.
func putRawSockaddr(destination *unix.RawSockaddrAny, address netip.AddrPort) (uint32, bool) {
	if !address.IsValid() {
		return 0, false
	}
	*destination = unix.RawSockaddrAny{}
	if address.Addr().Is4() {
		value := (*unix.RawSockaddrInet4)(unsafe.Pointer(destination))
		value.Family = unix.AF_INET
		value.Port = networkOrderUint16(address.Port())
		value.Addr = address.Addr().As4()
		return unix.SizeofSockaddrInet4, true
	}
	value := (*unix.RawSockaddrInet6)(unsafe.Pointer(destination))
	value.Family = unix.AF_INET6
	value.Port = networkOrderUint16(address.Port())
	value.Addr = address.Addr().As16()
	if zone := address.Addr().Zone(); zone != "" {
		index, ok := interfaceIndex(zone)
		if !ok {
			return 0, false
		}
		value.Scope_id = index
	}
	return unix.SizeofSockaddrInet6, true
}

// networkOrderUint16 converts a uint16 between native and network byte order.
func networkOrderUint16(value uint16) uint16 {
	var encoded [2]byte
	binary.BigEndian.PutUint16(encoded[:], value)
	return binary.NativeEndian.Uint16(encoded[:])
}

// interfaceName resolves a received IPv6 scope ID while preserving a numeric fallback.
func interfaceName(index uint32) string {
	if value, err := net.InterfaceByIndex(int(index)); err == nil {
		return value.Name
	}
	return strconv.FormatUint(uint64(index), 10)
}

// interfaceIndex resolves an IPv6 zone name or numeric scope ID.
func interfaceIndex(zone string) (uint32, bool) {
	if index, err := strconv.ParseUint(zone, 10, 32); err == nil {
		return uint32(index), true
	}
	value, err := net.InterfaceByName(zone)
	if err != nil {
		return 0, false
	}
	return uint32(value.Index), true
}
