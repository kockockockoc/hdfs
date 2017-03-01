package rpc

import (
	"encoding/binary"
	"io"

	hadoop "github.com/colinmarc/hdfs/protocol/hadoop_common"
	sasl "github.com/freddierice/go-sasl"
	"github.com/golang/protobuf/proto"
)

type saslConnection struct {
	*NamenodeConnection
	client *sasl.Client
}

func SASLConnect(c *NamenodeConnection) error {
	s := &saslConnection{c, nil}

	// negotiate
	if err := s.send(&hadoop.RpcSaslProto{State: hadoop.RpcSaslProto_NEGOTIATE.Enum()}); err != nil {
		return err
	}

ReadLoop:
	for {
		resp, err := s.recv()
		if err != nil {
			return err
		}

		// NEGOTIATE
		if resp.GetState() == hadoop.RpcSaslProto_NEGOTIATE {
			for _, auth := range resp.GetAuths() {
				if auth.GetMethod() == "KERBEROS" {
					saslClient, err := sasl.NewClient(auth.GetProtocol(), auth.GetServerId(), nil)
					if err != nil {
						return err
					}

					s.client = saslClient

					_, token, _, err := s.client.Start([]string{auth.GetMechanism()})
					if err != nil {
						return err
					}

					req := &hadoop.RpcSaslProto{
						State: hadoop.RpcSaslProto_INITIATE.Enum(),
						Token: token,
						Auths: []*hadoop.RpcSaslProto_SaslAuth{
							{
								Method:    auth.Method,
								Mechanism: auth.Mechanism,
								Protocol:  auth.Protocol,
								ServerId:  auth.ServerId,
							},
						},
					}

					if err = s.send(req); err != nil {
						return err
					}

					continue ReadLoop
				}
			}
		}

		// CHALLENGE
		if resp.GetState() == hadoop.RpcSaslProto_CHALLENGE {
			if s.client == nil {
				return &NamenodeError{
					Method:  "sasl",
					Message: "received CHALLENGE before NEGOTIATE",
				}
			}

			token, _, err := s.client.Step(resp.GetToken())
			if err != nil {
				return err
			}

			req := &hadoop.RpcSaslProto{
				State: hadoop.RpcSaslProto_RESPONSE.Enum(),
				Token: token,
			}

			if err = s.send(req); err != nil {
				return err
			}

			continue ReadLoop
		}

		if resp.GetState() == hadoop.RpcSaslProto_SUCCESS {
			break
		}

		return &NamenodeError{
			Method:  "sasl",
			Message: "Unexpected reply from namenode",
		}
	}

	return nil
}

func (s *saslConnection) send(message proto.Message) error {
	header := &hadoop.RpcRequestHeaderProto{
		RpcKind:    hadoop.RpcKindProto_RPC_PROTOCOL_BUFFER.Enum(),
		RpcOp:      hadoop.RpcRequestHeaderProto_RPC_FINAL_PACKET.Enum(),
		CallId:     proto.Int32(-33), // SASL
		RetryCount: proto.Int32(-1),
		ClientId:   []byte{},
	}

	reqBytes, err := makeRPCPacket(header, message)
	if err != nil {
		return err
	}

	_, err = s.conn.Write(reqBytes)
	return err
}

func (s *saslConnection) recv() (*hadoop.RpcSaslProto, error) {
	var packetLength uint32
	err := binary.Read(s.conn, binary.BigEndian, &packetLength)
	if err != nil {
		return nil, err
	}

	packet := make([]byte, packetLength)
	_, err = io.ReadFull(s.conn, packet)
	if err != nil {
		return nil, err
	}

	resp := &hadoop.RpcSaslProto{}
	rrh := &hadoop.RpcResponseHeaderProto{}
	err = readRPCPacket(packet, rrh, resp)

	if err != nil {
		return nil, err
	}

	if rrh.GetStatus() != hadoop.RpcResponseHeaderProto_SUCCESS {
		return nil, &NamenodeError{
			Method:    "sasl",
			Message:   rrh.GetErrorMsg(),
			Code:      int(rrh.GetErrorDetail()),
			Exception: rrh.GetExceptionClassName(),
		}
	}

	return resp, err
}
