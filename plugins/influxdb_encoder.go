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

	"github.com/mozilla-services/heka/message"
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
	msg := pack.Message
	buf := bytes.Buffer{}

	writeEscMeasure(&buf, *msg.Type)
	buf.WriteRune(',')
	buf.WriteString("Logger=")
	writeEscField(&buf, *msg.Logger)

	buf.WriteRune(' ')

	first := true
	for _, field := range msg.Fields {
		if first == false {
			buf.WriteRune(',')
		} else {
			first = false
		}
		writeEscField(&buf, *field.Name)
		buf.WriteRune('=')

		switch field.GetValueType() {
		case message.Field_INTEGER:
			buf.WriteString(strconv.FormatInt(field.GetValueInteger(), 10))
			buf.WriteRune('i')
		case message.Field_DOUBLE:
			buf.WriteString(strconv.FormatFloat(field.GetValueDouble(), 'f', -1, 64))
		case message.Field_BOOL:
			buf.WriteString(strconv.FormatBool(field.GetValueBool()))
		case message.Field_STRING:
			buf.WriteRune('"')
			writeEscField(&buf, field.GetValueString())
			buf.WriteRune('"')
		default:
			err = fmt.Errorf("Unsupported field type: %s: %s",
				*field.Name, message.Field_ValueType_name[int32(field_type)])
			return
		}
	}

	buf.WriteRune(' ')
	buf.WriteString(strconv.FormatInt(time.Unix(0, msg.GetTimestamp()).UnixNano(), 10))
	buf.WriteRune('\n')
	return
}

func init() {
	pipeline.RegisterPlugin("InfluxdbEncoder", func() interface{} {
		return new(InfluxdbEncoder)
	})
}
