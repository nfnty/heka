/***** BEGIN LICENSE BLOCK *****
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# The Initial Developer of the Original Code is the Mozilla Foundation.
# Portions created by the Initial Developer are Copyright (C) 2014
# the Initial Developer. All Rights Reserved.
#
# Contributor(s):
#   Rob Miller (rmiller@mozilla.com)
#
# ***** END LICENSE BLOCK *****/

package plugins

import (
	"bytes"
	"fmt"
	"reflect"
	"strconv"
	"time"

	"github.com/mozilla-services/heka/pipeline"
)

type InfluxdbEncoder struct{}

func (ie *InfluxdbEncoder) Init(config interface{}) error {
	return nil
}

func writeEscMeasure(buf *bytes.Buffer, str string) {
	for _, r := range str {
		if r == ',' || r == ' ' {
			buf.WriteRune('\\')
		}
		buf.WriteRune(r)
	}
}

func writeEscField(buf *bytes.Buffer, str string) {
	for _, r := range str {
		if r == ',' || r == ' ' || r == '=' {
			buf.WriteRune('\\')
		}
		buf.WriteRune(r)
	}
}

func (ie *InfluxdbEncoder) Encode(pack *pipeline.PipelinePack) (output []byte, err error) {
	message := pack.Message
	buf := bytes.Buffer{}

	writeEscMeasure(buf, message.Type)
	buf.WriteRune(',')
	buf.WriteString("Logger=")
	writeEscField(buf, message.Logger)

	buf.WriteRune(' ')

	first := true
	for _, field := range message.Fields {
		key = field.Name
		value = field.GetValue()

		if first == false {
			buf.WriteRune(',')
		} else {
			first = false
		}
		writeEscField(buf, key)
		buf.WriteRune('=')

		kind := reflect.TypeOf(value).Kind()
		switch kind {
		case reflect.Int:
			buf.WriteString(strconv.FormatInt(value, 10))
			buf.WriteRune('i')
		case reflect.Uint:
			buf.WriteString(strconv.FormatUint(value, 10))
			buf.WriteRune('i')
		case reflect.Float32:
			buf.WriteString(strconv.FormatFloat(value, 'f', -1, 32))
		case reflect.Float64:
			buf.WriteString(strconv.FormatFloat(value, 'f', -1, 64))
		case reflect.Bool:
			buf.WriteString(strconv.FormatBool(value))
		case reflect.String:
			buf.WriteRune('"')
			writeEscField(buf, value)
			buf.WriteRune('"')
		default:
			err = fmt.Errorf("Unknown value type: %s: %s", key, kind)
			return
		}
	}

	buf.WriteRune(' ')
	buf.WriteString(strconv.FormatInt(time.Unix(0, message.GetTimestamp()).UnixNano(), 10))
	buf.WriteRune('\n')
	return
}

func init() {
	pipeline.RegisterPlugin("InfluxdbEncoder", func() interface{} {
		return new(InfluxdbEncoder)
	})
}
