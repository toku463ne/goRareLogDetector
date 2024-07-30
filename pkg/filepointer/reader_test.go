package filepointer

import (
	"testing"
)

func TestReader_basic(t *testing.T) {
	type fields struct {
		infile string
	}
	tests := []struct {
		name      string
		fields    fields
		wantCount int
		wantErr   bool
	}{
		{"textfile", fields{"../../test/data/filepointer/reader_sample.txt"}, 6, false},
		{"gzipfile", fields{"../../test/data/filepointer/reader_sample.txt.gz"}, 6, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lr, err := newReader(tt.fields.infile)
			if (err != nil) != tt.wantErr {
				t.Errorf("newReader error = %v, wantErr %v", err, tt.wantErr)
			}
			defer lr.close()
			cnt := 0
			for lr.next() {
				//fmt.Printf(lr.text())
				cnt++
			}
			err = lr.err()
			if (err != nil) != tt.wantErr {
				t.Errorf("newLogReader error = %v, wantErr %v", err, tt.wantErr)
			}
			if cnt != tt.wantCount {
				t.Errorf("newLogReader filecount want=%d got=%d", tt.wantCount, cnt)
			}
		})
	}
}
