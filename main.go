package main

func main() {
	e := NewELS("./buckets")
	je := JsonELS{}
	je.els = e
	jsonData := "{\"TestObject\": {\"TestArray\":[1234,123,12,1],\"TestField1\":\"TestLiteral\",\"TestField2\":123456}}"
	println("Writing: " + jsonData)
	err := je.WriteJSON([]byte(jsonData), "TestPrefix")

	jsonSchema := "{\"TestObject\": {\"TestArray\" : [0,0,0,0], \"TestField1\": \"\", \"TestField2\": 0}}"
	value, err := je.ReadJSON([]byte(jsonSchema), "TestPrefix")
	if err != nil {
		println(err.Error())
		return
	}
	println("")
	println("")
	println("Using scheme: " + jsonSchema)
	println("Read: " + string(value))
}
