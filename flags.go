package hdfs

type Flag uint32

const (
	NoReplace   Flag = 1
	NoRecursive Flag = 2
)

func mergeFlags(flags []Flag) Flag {
	var flag Flag = 0
	for _, f := range flags {
		flag |= f
	}
	return flag
}
