package nsqlookupd

import (
	"math"
	"strconv"
	"strings"
)

const (
	MapType_Bitmap = 1
)

type UsageMap struct {
	Start   int
	End     int
	MapSize int
	MapType int
	UseMap  []byte
}

func NewAddrMap(min string, max string) *UsageMap {
	start := IP4toInt(min)
	end := IP4toInt(max)
	mapSize := end - start
	n := int(math.Ceil(float64(mapSize) / 8))
	return &UsageMap{
		Start:   start,
		End:     end,
		MapSize: mapSize,
		MapType: MapType_Bitmap,
		UseMap:  make([]byte, n, n),
	}
}

func NewPortMap(min int, max int) *UsageMap {
	mapSize := max - min
	return &UsageMap{
		Start:   min,
		End:     max,
		MapSize: mapSize,
		MapType: MapType_Bitmap,
		UseMap:  make([]byte, mapSize, mapSize),
	}
}

func InttoIP4(ip int) string {
	b0 := (ip >> 24) & 0xff
	b1 := (ip >> 16) & 0xff
	b2 := (ip >> 8) & 0xff
	b3 := ip & 0xff
	return strconv.Itoa(b0) + "." + strconv.Itoa(b1) + "." + strconv.Itoa(b2) + "." + strconv.Itoa(b3)
}

func IP4toInt(addr string) int {
	bits := strings.Split(addr, ".")
	b0, _ := strconv.Atoi(bits[0])
	b1, _ := strconv.Atoi(bits[1])
	b2, _ := strconv.Atoi(bits[2])
	b3, _ := strconv.Atoi(bits[3])
	var sum int
	sum += int(b0) << 24
	sum += int(b1) << 16
	sum += int(b2) << 8
	sum += int(b3)
	return sum
}

func (m *UsageMap) GetNew() int {
	if m.MapType == MapType_Bitmap {
		for i := 0; i < len(m.UseMap); i++ {
			if m.UseMap[i] == 0xff {
				continue
			}
			for j := 0; j < 8; j++ {
				if m.UseMap[i]&(uint8(0x1)<<uint(j)) == 0 {
					m.UseMap[i] = m.UseMap[i] | (uint8(0x1) << uint(j))
					if i+m.Start >= m.End {
						continue
					}
					return i*8 + j + m.Start
				}
			}
		}
	}
	return -1
}

func (m *UsageMap) RemoveUsage(use int) {
	index := use - m.Start
	if m.MapType == MapType_Bitmap {
		i := index / 8
		j := uint(index % 8)
		m.UseMap[i] = m.UseMap[i] & ^(uint8(0x1) << j)
	}
}
