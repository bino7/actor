package actor
import (
    "encoding/gob"
    "bytes"
)

func GobEncode(value interface{})([]byte,error){
    buf := new(bytes.Buffer)
    enc := gob.NewEncoder(buf)
    err := enc.Encode(value)
    return buf.Bytes(),err
}

func GobDecode(data []byte,value interface{})(interface{},error){
    buf := bytes.NewBuffer(data)
    dec := gob.NewDecoder(buf)
    err:=dec.Decode(value)
    return value,err
}