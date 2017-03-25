package hdfs

import (
	"os"

	hdfs "github.com/colinmarc/hdfs/protocol/hadoop_hdfs"
	"github.com/colinmarc/hdfs/rpc"
	"github.com/golang/protobuf/proto"
)

// Rename renames (moves) a file.
func (c *Client) Rename(oldpath, newpath string, flags ...Flag) error {
	flag := mergeFlags(flags)
	flagOverwriteDest := true
	if flag&NoReplace != 0 {
		flagOverwriteDest = false
	}

	_, err := c.getFileInfo(newpath)
	if err != nil && !os.IsNotExist(err) {
		return &os.PathError{"rename", newpath, err}
	}

	req := &hdfs.Rename2RequestProto{
		Src:           proto.String(oldpath),
		Dst:           proto.String(newpath),
		OverwriteDest: proto.Bool(flagOverwriteDest),
	}
	resp := &hdfs.Rename2ResponseProto{}

	err = c.namenode.Execute("rename2", req, resp)
	if err != nil {
		if nnErr, ok := err.(*rpc.NamenodeError); ok {
			err = interpretException(nnErr.Exception, err)
		}

		return &os.PathError{"rename", oldpath, err}
	}

	return nil
}
