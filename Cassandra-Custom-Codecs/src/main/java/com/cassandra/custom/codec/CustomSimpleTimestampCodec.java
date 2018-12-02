package com.cassandra.custom.codec;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.core.exceptions.InvalidTypeException;

import java.nio.ByteBuffer;
import java.text.ParseException;

import static com.datastax.driver.core.ParseUtils.*;
import static java.lang.Long.parseLong;

public class CustomSimpleTimestampCodec extends TypeCodec.PrimitiveLongCodec {

	public static final CustomSimpleTimestampCodec instance = new CustomSimpleTimestampCodec();

	public CustomSimpleTimestampCodec() {
		super(DataType.timestamp());
	}

	@Override
	public ByteBuffer serializeNoBoxing(long value, ProtocolVersion protocolVersion) {
		System.out.println("input value : "+value);
		ByteBuffer bb = ByteBuffer.allocate(8);
		bb.putLong(0, value);
		return bb;
	}

	@Override
	public long deserializeNoBoxing(ByteBuffer bytes, ProtocolVersion protocolVersion) {
		if (bytes == null || bytes.remaining() == 0)
			return 0;
		if (bytes.remaining() != 8)
			throw new InvalidTypeException(
					"Invalid 64-bits long value, expecting 8 bytes but got " + bytes.remaining());
		return bytes.getLong(bytes.position());
	}

	@Override
	public Long parse(String value) {
		System.out.println(" parse input value : "+value);
		if (value == null || value.isEmpty() || value.equalsIgnoreCase("NULL"))
			return null;

		// single quotes are optional for long literals, mandatory for date patterns
		// strip enclosing single quotes, if any
		if (isQuoted(value))
			value = unquote(value);

		if (isLongLiteral(value)) {
			try {
				return parseLong(value);
			} catch (NumberFormatException e) {
				throw new InvalidTypeException(String.format("Cannot parse timestamp value from \"%s\"", value), e);
			}
		}

		try {
			return parseDate(value).getTime();
		} catch (ParseException e) {
			throw new InvalidTypeException(String.format("Cannot parse timestamp value from \"%s\"", value), e);
		}
	}

	@Override
	public String format(Long value) {
		if (value == null)
			return "NULL";
		return quote(Long.toString(value));
	}

}
