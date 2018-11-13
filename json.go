package main

import (
	"encoding/json"
	"sort"
	"strconv"
)

type JsonELS struct {
	els *ELS
}

type JsonPath struct {
	path  []string
	value []byte
}

func (jels *JsonELS) SplitJSON(jsonData []byte, prefix string) ([]JsonPath, error) {
	paths, err := getAllJSONPaths(jels.els, jsonData, []string{prefix})
	if err != nil {
		return nil, err
	}
	return paths, err
}

func (jels *JsonELS) WriteJSON(jsonData []byte, prefix string) error {
	paths, err := jels.SplitJSON(jsonData, prefix)
	if err != nil {
		return err
	}
	for _, p := range paths {
		bkt, err := jels.els.OpenBucket(p.path[0], p.path[1:]...)
		if err != nil {
			return err
		}

		wrtn, err := bkt.Write(p.value)
		if err != nil {
			return err
		}
		if wrtn != int64(len(p.value)) {
			panic("AAAA PANIC!")
		}
	}
	return nil
}

func smaller(s1, s2 string) bool {
	for i := 0; i < len(s1); i++ {
		if s1[i] < s2[i] {
			return true
		}
		if s1[i] > s2[i] {
			return false
		}
	}
	//one of s1 or s2 is a prefix of the other
	return len(s1) < len(s2)
}

func smallerPath(p1, p2 []string) bool {
	for i := 0; i < len(p1) && i < len(p2); i++ {
		if p1[i] == p2[i] {
			continue
		}
		return smaller(p1[i], p2[i])
	}
	//one is prefix of the other
	return len(p1) < len(p2)
}

//check path but ignore last element
func equalButLastPath(p1, p2 []string) bool {
	if len(p1) != len(p2) {
		return false
	}

	for i := 0; i < len(p1)-1 && i < len(p2)-1; i++ {
		if p1[i] == p2[i] {
			continue
		}
		return false
	}
	return true
}

func equalPath(p1, p2 []string) bool {
	if len(p1) != len(p2) {
		return false
	}

	for i := 0; i < len(p1) && i < len(p2); i++ {
		if p1[i] == p2[i] {
			continue
		}
		return false
	}
	return true
}

func joinJsonPaths(paths []JsonPath) []JsonPath {

	merged := paths
	for len(merged) > 1 {
		sort.Slice(merged, func(i, j int) bool {
			return !smallerPath(merged[i].path, merged[j].path)
		})

		from := 0
		to := 0

		result := make([]byte, 0)

		for to < len(merged) && equalButLastPath(merged[from].path, merged[to].path) {
			//find group of matching paths
			to++
		}

		//check if the found group is an array
		treatAsArray := false
		for i := from; i < to; i++ {
			if merged[i].path[len(merged[i].path)-1] == "ARRAY" {
				treatAsArray = true
				break
			}
		}

		//if the group is an array, merge as an array
		if treatAsArray {
			result = append(result, '[')
			for i := from; i < to; i++ {
				if merged[i].path[len(merged[i].path)-1] == "ARRAY" {
					continue //skip sentinel
				}
				for _, c := range merged[i].value {
					result = append(result, c)
				}
				result = append(result, ',')
			}
			result[len(result)-1] = ']'
		} else {

			//collect parts of objects in the map. if the key matches the need to get merged
			elementMap := make(map[string][]byte)
			for i := from; i < to; i++ {
				elName := merged[i].path[len(merged[i].path)-1]
				existing, ok := elementMap[elName]
				if !ok {
					elementMap[elName] = merged[i].value
				} else {
					existing[len(existing)-1] = ','
					merged[i].value[0] = ' '
					for _, c := range merged[i].value {
						existing = append(existing, c)
					}
					elementMap[elName] = existing
				}
			}

			result = append(result, '{')
			for key, value := range elementMap {
				result = append(result, '"')
				for _, c := range []byte(key) {
					result = append(result, c)
				}
				result = append(result, '"')
				result = append(result, ':')
				for _, c := range value {
					result = append(result, c)
				}
				result = append(result, ',')
			}
			result[len(result)-1] = '}'
		}

		newPath := JsonPath{path: merged[from].path[:len(merged[from].path)-1], value: result}
		merged = append(merged, newPath)

		merged = merged[to:]
	}
	return merged
}

func (jels *JsonELS) ReadJSON(jsonSchema []byte, prefix string) ([]byte, error) {
	paths, err := jels.SplitJSON(jsonSchema, prefix)
	if err != nil {
		return nil, err
	}
	for idx, _ := range paths {
		bkt, err := jels.els.OpenBucket(paths[idx].path[0], paths[idx].path[1:]...)
		if err != nil {
			return nil, err
		}

		paths[idx].value, err = bkt.ReadValue()
		if err != nil {
			return nil, err
		}
	}
	paths = joinJsonPaths(paths)
	return paths[0].value, nil
}

func getAllJSONPaths(els *ELS, jsonData []byte, pathInJSON []string) ([]JsonPath, error) {
	result := make([]JsonPath, 0)

	thisLevel := make(map[string]json.RawMessage)
	err := json.Unmarshal(jsonData, &thisLevel)
	if err == nil {
		for key, value := range thisLevel {
			res, err := getAllJSONPaths(els, []byte(value), append(pathInJSON, key))
			if err != nil {
				return nil, err
			}
			for _, r := range res {
				result = append(result, r)
			}
		}
		return result, nil
	}

	array := make([]json.RawMessage, 0)
	err = json.Unmarshal(jsonData, &array)
	if err == nil {
		for key, value := range array {
			res, err := getAllJSONPaths(els, []byte(value), append(pathInJSON, strconv.Itoa(key)))
			if err != nil {
				return nil, err
			}
			for _, r := range res {
				result = append(result, r)
			}
		}
		arraySentinel := JsonPath{}
		sentPath := make([]string, len(pathInJSON)+1)
		copy(sentPath, pathInJSON)
		sentPath[len(sentPath)-1] = "ARRAY"
		arraySentinel.path = sentPath
		result = append(result, arraySentinel)
		return result, nil
	}

	respath := make([]string, len(pathInJSON))
	copy(respath, pathInJSON)

	res := JsonPath{path: respath, value: []byte(jsonData)}
	return []JsonPath{res}, nil
}
