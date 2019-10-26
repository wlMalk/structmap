package structmap

import (
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"unicode"
	"unicode/utf8"

	"github.com/iancoleman/orderedmap"
)

type cache struct {
	lock           sync.RWMutex
	infos          map[reflect.Type]*structInfo
	forceOmitEmpty bool
	nameTags       []string
	nameFunc       func(name string) string
}

type structInfo struct {
	fields []*fieldInfo
}

type fieldInfo struct {
	structName          string
	name                string
	alias               string
	whole               bool
	omitEmpty           bool
	merge               bool
	collectionOfStructs bool
	typ                 reflect.Type
	tag                 reflect.StructTag
}

func (c *cache) get(t reflect.Type) *structInfo {
	c.lock.RLock()
	info := c.infos[t]
	c.lock.RUnlock()
	if info == nil {
		info = c.create(t)
		c.lock.Lock()
		c.infos[t] = info
		c.lock.Unlock()
	}
	return info
}

func isExported(name string) bool {
	ch, _ := utf8.DecodeRuneInString(name)
	return unicode.IsUpper(ch)
}

func (c *cache) create(t reflect.Type) *structInfo {
	sName := t.Name()
	info := &structInfo{}
	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		if c.isHidden(field) || !isExported(field.Name) {
			continue
		}
		fInfo := &fieldInfo{
			structName:          sName,
			name:                field.Name,
			alias:               c.alias(field),
			omitEmpty:           c.isOmitEmpty(field),
			whole:               c.hasStructOption(field, "whole"),
			typ:                 field.Type,
			tag:                 field.Tag,
			collectionOfStructs: isCollectionOfStructs(field.Type),
		}
		if fInfo.collectionOfStructs && fInfo.alias == "" {
			fInfo.merge = false
			if c.nameFunc != nil {
				if alias := c.nameFunc(fInfo.name); alias != "" {
					fInfo.alias = alias
				}
			} else {
				fInfo.alias = fInfo.name
			}
		}
		fInfo.merge = !fInfo.collectionOfStructs && ((field.Anonymous && !c.hasAlias(field)) || c.hasStructOption(field, "merge") || fInfo.alias == "")
		info.fields = append(info.fields, fInfo)

	}

	return info
}

func (c *cache) hasAlias(f reflect.StructField) bool {
	if len(c.nameTags) > 0 {
		for i := range c.nameTags {
			tag, ok := f.Tag.Lookup(c.nameTags[i])
			s := strings.Split(tag, ",")
			if ok && tag != "" && s[0] != "" {
				return true
			}
		}
		return false
	}
	return true
}

func (c *cache) alias(f reflect.StructField) (alias string) {
	if len(c.nameTags) > 0 {
		for i := range c.nameTags {
			tag, ok := f.Tag.Lookup(c.nameTags[i])
			s := strings.Split(tag, ",")
			if tag != "" && s[0] != "" {
				return s[0]
			} else if ok {
				return ""
			}
		}
	}
	if c.nameFunc != nil {
		if alias = c.nameFunc(f.Name); alias != "" {
			return
		}
	}
	return f.Name
}

func (c *cache) isHidden(f reflect.StructField) bool {
	for i := range c.nameTags {
		if f.Tag.Get(c.nameTags[i]) == "-" {
			return true
		}
	}
	return false
}

func (c *cache) isOmitEmpty(f reflect.StructField) bool {
	if c.forceOmitEmpty {
		return true
	}

	for i := range c.nameTags {
		s := strings.Split(f.Tag.Get(c.nameTags[i]), ",")
		if len(s) < 2 {
			continue
		}
		for j := 1; j < len(s); j++ {
			if s[j] == "omitempty" {
				return true
			}
		}
	}

	return false
}

func (c *cache) hasStructOption(f reflect.StructField, option string) bool {
	if it(f.Type).Kind() != reflect.Struct {
		return false
	}

	for i := range c.nameTags {
		s := strings.Split(f.Tag.Get(c.nameTags[i]), ",")
		if len(s) < 2 {
			continue
		}
		for j := 1; j < len(s); j++ {
			if s[j] == option {
				return true
			}
		}
	}
	return false
}

func (i *structInfo) get(alias string) *fieldInfo {
	for _, field := range i.fields {
		if strings.EqualFold(field.alias, alias) {
			return field
		}
	}
	return nil
}

func containsAlias(infos []*structInfo, alias string) bool {
	for _, info := range infos {
		if info.get(alias) != nil {
			return true
		}
	}
	return false
}

func isCollectionOfStructs(t reflect.Type) bool {
	if it(t).Kind() == reflect.Struct {
		return false
	}
	for t.Kind() == reflect.Slice ||
		t.Kind() == reflect.Array ||
		t.Kind() == reflect.Ptr ||
		t.Kind() == reflect.Map {
		t = t.Elem()
	}
	return t.Kind() == reflect.Struct
}

// returns indirect type if its a pointer or returns the given type
func it(typ reflect.Type) reflect.Type {
	if typ.Kind() == reflect.Ptr {
		return typ.Elem()
	}
	return typ
}

// returns indirect value if its a pointer or returns the given value
func iv(val reflect.Value) reflect.Value {
	if val.Kind() == reflect.Ptr {
		return val.Elem()
	}
	return val
}

type Mapper struct {
	cache *cache
}

type (
	IncludeFunc func(name string, path string, tag reflect.StructTag) bool
	AliasFunc   func(name string, path string) string
)

func newCache() *cache {
	return &cache{
		infos: map[reflect.Type]*structInfo{},
	}
}

func New() *Mapper {
	return &Mapper{
		cache: newCache(),
	}
}

func (m *Mapper) NameTag(n ...string) {
	m.cache.nameTags = n
}

func (m *Mapper) NameFunc(n func(string) string) {
	m.cache.nameFunc = n
}

func (m *Mapper) ForceOmitEmpty(f bool) {
	m.cache.forceOmitEmpty = f
}

func (m *Mapper) Map(s interface{}, ifn IncludeFunc, afn AliasFunc) (*orderedmap.OrderedMap, error) {
	om := orderedmap.New()
	err := m.mapInto(s, om, "", ifn, afn)
	if err != nil {
		return nil, err
	}
	return om, nil
}

func (m *Mapper) mapInto(s interface{}, om *orderedmap.OrderedMap, path string, ifn IncludeFunc, afn AliasFunc) error {
	if s == nil {
		return nil
	}

	v := iv(reflect.ValueOf(s))
	t := it(reflect.TypeOf(s))
	if !v.IsValid() {
		return nil
	}
	if v.Kind() != reflect.Struct {
		return errors.New("value not a struct or pointer to a struct")
	}

	info := m.cache.get(t)
	var err error

	for _, f := range info.fields {
		vField := iv(v.FieldByName(f.name))
		tField := it(f.typ)
		key := f.alias

		if !f.merge && ifn != nil && !ifn(key, path, f.tag) {
			continue
		}

		if vField.Kind() == reflect.Struct {
			if f.merge && !f.whole {
				err = m.mapInto(vField.Interface(), om, path, ifn, afn)
				if err != nil {
					return err
				}
			} else if f.whole {
				m.set(m.aliasOr(key, path, afn), vField, om, f)
			} else {
				mn := orderedmap.New()
				err = m.mapInto(vField.Interface(), mn, join(path, key), ifn, afn)
				if err != nil {
					return err
				}
				m.setOrderedMap(m.aliasOr(key, path, afn), mn, om, f)
			}
		} else if f.merge && !f.whole && vField.Kind() == reflect.Map {
			for _, k := range vField.MapKeys() {
				mkey := m.nameOr(fmt.Sprint(k))
				if ifn != nil && !ifn(mkey, path, f.tag) {
					continue
				}
				m.set(m.aliasOr(mkey, path, afn), vField.MapIndex(k), om, f)
			}
		} else if tField.Kind() == reflect.Array || tField.Kind() == reflect.Slice {
			a, err := m.extractSlice(tField, vField, f, join(path, key), ifn, afn)
			if err != nil {
				return err
			}
			m.setSlice(m.aliasOr(key, path, afn), a, om, f)
		} else if tField.Kind() == reflect.Map {
			a, err := m.extractMap(tField, vField, f, join(path, key), ifn, afn)
			if err != nil {
				return err
			}
			m.setMap(m.aliasOr(key, path, afn), a, om, f)
		} else {
			m.set(m.aliasOr(key, path, afn), vField, om, f)
		}
	}
	return nil
}

func (m *Mapper) extractMap(t reflect.Type, v reflect.Value, f *fieldInfo, path string, ifn IncludeFunc, afn AliasFunc) (map[string]interface{}, error) {
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
		v = v.Elem()
	}
	a := map[string]interface{}{}
	if t.Elem().Kind() == reflect.Struct || (t.Elem().Kind() == reflect.Ptr && t.Elem().Elem().Kind() == reflect.Struct) {
		for _, k := range v.MapKeys() {
			key := m.nameOr(fmt.Sprint(k))
			mPath := join(path, key)
			if ifn != nil && !ifn(key, path, f.tag) {
				continue
			}
			mn := orderedmap.New()
			vk := v.MapIndex(k)
			err := m.mapInto(vk.Interface(), mn, mPath, ifn, afn)
			if err != nil {
				return nil, err
			}
			if len(mn.Keys()) == 0 {
				mn = nil
			}
			a[m.aliasOr(key, path, afn)] = mn
		}
	} else if t.Elem().Kind() == reflect.Slice || t.Elem().Kind() == reflect.Array || (t.Elem().Kind() == reflect.Ptr &&
		(t.Elem().Elem().Kind() == reflect.Slice || t.Elem().Elem().Kind() == reflect.Array)) {
		for _, k := range v.MapKeys() {
			key := m.nameOr(fmt.Sprint(k))
			mPath := join(path, key)
			if ifn != nil && !ifn(key, path, f.tag) {
				continue
			}
			vk := iv(v.MapIndex(k))
			if f.omitEmpty && vk.Len() == 0 {
				continue
			}
			sl, err := m.extractSlice(vk.Type(), vk, f, mPath, ifn, afn)
			if err != nil {
				return nil, err
			}
			if f.omitEmpty && len(sl) == 0 {
				continue
			}
			a[m.aliasOr(key, path, afn)] = sl
		}
	} else if t.Elem().Kind() == reflect.Map || (t.Elem().Kind() == reflect.Ptr && t.Elem().Elem().Kind() == reflect.Map) {
		for _, k := range v.MapKeys() {
			key := m.nameOr(fmt.Sprint(k))
			mPath := join(path, key)
			if ifn != nil && !ifn(key, path, f.tag) {
				continue
			}
			vk := v.MapIndex(k)
			if f.omitEmpty && vk.Len() == 0 {
				continue
			}
			mn, err := m.extractMap(vk.Type(), vk, f, mPath, ifn, afn)
			if err != nil {
				return nil, err
			}
			if f.omitEmpty && len(mn) == 0 {
				continue
			}
			a[m.aliasOr(key, path, afn)] = mn
		}
	} else {
		for _, k := range v.MapKeys() {
			key := m.nameOr(fmt.Sprint(k))
			if ifn != nil && !ifn(key, path, f.tag) {
				continue
			}
			vk := v.MapIndex(k)
			if !vk.CanInterface() {
				continue
			}
			a[m.aliasOr(key, path, afn)] = vk.Interface()
		}
	}
	return a, nil
}

func (m *Mapper) extractSlice(t reflect.Type, v reflect.Value, f *fieldInfo, path string, ifn IncludeFunc, afn AliasFunc) ([]interface{}, error) {
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
		v = v.Elem()
	}
	a := make([]interface{}, 0, v.Len())
	if t.Elem().Kind() == reflect.Struct || (t.Elem().Kind() == reflect.Ptr && t.Elem().Elem().Kind() == reflect.Struct) {
		for i := 0; i < v.Len(); i++ {
			mn := orderedmap.New()
			vi := v.Index(i)
			err := m.mapInto(vi.Interface(), mn, join(path, "*"), ifn, afn)
			if err != nil {
				return nil, err
			}
			if len(mn.Keys()) == 0 {
				mn = nil
			}
			a = append(a, mn)
		}
	} else if t.Elem().Kind() == reflect.Slice || t.Elem().Kind() == reflect.Array || (t.Elem().Kind() == reflect.Ptr &&
		(t.Elem().Elem().Kind() == reflect.Slice || t.Elem().Elem().Kind() == reflect.Array)) {
		for i := 0; i < v.Len(); i++ {
			vi := v.Index(i)
			if f.omitEmpty && vi.Len() == 0 {
				continue
			}
			sl, err := m.extractSlice(vi.Type(), vi, f, join(path, "*"), ifn, afn)
			if err != nil {
				return nil, err
			}
			if f.omitEmpty && len(sl) == 0 {
				continue
			}
			a = append(a, sl)
		}
	} else if t.Elem().Kind() == reflect.Map || (t.Elem().Kind() == reflect.Ptr && t.Elem().Elem().Kind() == reflect.Map) {
		for i := 0; i < v.Len(); i++ {
			vi := v.Index(i)
			if f.omitEmpty && vi.Len() == 0 {
				continue
			}
			mp, err := m.extractMap(vi.Type(), vi, f, join(path, "*"), ifn, afn)
			if err != nil {
				return nil, err
			}
			if f.omitEmpty && len(mp) == 0 {
				continue
			}
			a = append(a, mp)
		}
	} else {
		for i := 0; i < v.Len(); i++ {
			vi := v.Index(i)
			if !vi.CanInterface() {
				continue
			}
			a = append(a, vi.Interface())
		}
	}
	return a, nil
}

func (m *Mapper) nameOr(a string) string {
	if m.cache.nameFunc != nil {
		return m.cache.nameFunc(a)
	}
	return a
}

func (m *Mapper) aliasOr(name string, path string, afn AliasFunc) string {
	if afn != nil {
		if a := afn(name, path); a != "" {
			return a
		}
	}
	return name
}

func (m *Mapper) set(key string, v reflect.Value, om *orderedmap.OrderedMap, field *fieldInfo) {
	if field.omitEmpty && (v.IsZero() || !v.CanInterface()) {
		return
	}
	om.Set(key, v.Interface())
}

func (m *Mapper) setSlice(key string, v []interface{}, om *orderedmap.OrderedMap, field *fieldInfo) {
	if field.omitEmpty && len(v) == 0 {
		return
	}
	om.Set(key, v)
}

func (m *Mapper) setMap(key string, v map[string]interface{}, om *orderedmap.OrderedMap, field *fieldInfo) {
	if field.omitEmpty && len(v) == 0 {
		return
	}
	om.Set(key, v)
}

func (m *Mapper) setOrderedMap(key string, v *orderedmap.OrderedMap, om *orderedmap.OrderedMap, field *fieldInfo) {
	if field.omitEmpty && (v == nil || len(v.Keys()) == 0) {
		return
	}
	om.Set(key, v)
}

func join(path, alias string) string {
	if path == "" {
		return alias
	}
	return path + "." + alias
}
