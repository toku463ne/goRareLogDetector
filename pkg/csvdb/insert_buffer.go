package csvdb

type insertBuff struct {
	rows   [][]string
	pos    int
	isFull bool
	size   int
}

func newInsertBuffer(bufferSize int) *insertBuff {
	b := new(insertBuff)
	b.size = bufferSize
	b.init()
	return b
}

func (b *insertBuff) init() {
	b.pos = -1
	if b.size == 0 {
		b.rows = make([][]string, cDefaultBuffSize)
	} else {
		b.rows = make([][]string, b.size)
	}
	b.isFull = false
}

func (b *insertBuff) register(row []string) bool {
	if b.isFull {
		return b.isFull
	}
	b.pos++
	b.rows[b.pos] = row

	if b.size == 0 {
		if b.pos+1 >= len(b.rows) {
			b.rows = append(b.rows, make([][]string, cDefaultBuffSize)...)
		}
	} else if b.pos+1 >= len(b.rows) {
		b.isFull = true
	}
	return b.isFull
}
func (b *insertBuff) setBuff(rows [][]string) {
	b.rows = rows
	b.pos = len(rows) - 1
}
