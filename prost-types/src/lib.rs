#![doc(html_root_url = "https://docs.rs/prost-types/0.6.1")]

//! Protocol Buffers well-known types.
//!
//! Note that the documentation for the types defined in this crate are generated from the Protobuf
//! definitions, so code examples are not in Rust.
//!
//! See the [Protobuf reference][1] for more information about well-known types.
//!
//! [1]: https://developers.google.com/protocol-buffers/docs/reference/google.protobuf

#![cfg_attr(not(feature = "std"), no_std)]

use std::borrow::Cow;
use core::convert::TryFrom;
use core::i32;
use core::i64;
use core::time;

use chrono::prelude::*;

use core::fmt;
use serde::ser::{Serialize, Serializer, SerializeSeq, SerializeMap, SerializeStruct};
use serde::de::{self, Deserialize, Deserializer, Visitor, SeqAccess, MapAccess};
use std::collections::BTreeMap;
use std::str::FromStr;

include!("protobuf.rs");
pub mod compiler {
    include!("compiler.rs");
}

// The Protobuf `Duration` and `Timestamp` types can't delegate to the standard library equivalents
// because the Protobuf versions are signed. To make them easier to work with, `From` conversions
// are defined in both directions.

const NANOS_PER_SECOND: i32 = 1_000_000_000;

impl Duration {
    /// Normalizes the duration to a canonical format.
    ///
    /// Based on [`google::protobuf::util::CreateNormalized`][1].
    /// [1]: https://github.com/google/protobuf/blob/v3.3.2/src/google/protobuf/util/time_util.cc#L79-L100
    pub fn normalize(&mut self) {
        // Make sure nanos is in the range.
        if self.nanos <= -NANOS_PER_SECOND || self.nanos >= NANOS_PER_SECOND {
            self.seconds += (self.nanos / NANOS_PER_SECOND) as i64;
            self.nanos %= NANOS_PER_SECOND;
        }

        // nanos should have the same sign as seconds.
        if self.seconds < 0 && self.nanos > 0 {
            self.seconds += 1;
            self.nanos -= NANOS_PER_SECOND;
        } else if self.seconds > 0 && self.nanos < 0 {
            self.seconds -= 1;
            self.nanos += NANOS_PER_SECOND;
        }
        // TODO: should this be checked?
        // debug_assert!(self.seconds >= -315_576_000_000 && self.seconds <= 315_576_000_000,
        //               "invalid duration: {:?}", self);
    }
}

/// Converts a `std::time::Duration` to a `Duration`.
impl From<time::Duration> for Duration {
    fn from(duration: time::Duration) -> Duration {
        let seconds = duration.as_secs();
        let seconds = if seconds > i64::MAX as u64 {
            i64::MAX
        } else {
            seconds as i64
        };
        let nanos = duration.subsec_nanos();
        let nanos = if nanos > i32::MAX as u32 {
            i32::MAX
        } else {
            nanos as i32
        };
        let mut duration = Duration { seconds, nanos };
        duration.normalize();
        duration
    }
}

impl TryFrom<Duration> for time::Duration {
    type Error = time::Duration;

    /// Converts a `Duration` to a result containing a positive (`Ok`) or negative (`Err`)
    /// `std::time::Duration`.
    fn try_from(mut duration: Duration) -> Result<time::Duration, time::Duration> {
        duration.normalize();
        if duration.seconds >= 0 {
            Ok(time::Duration::new(
                duration.seconds as u64,
                duration.nanos as u32,
            ))
        } else {
            Err(time::Duration::new(
                (-duration.seconds) as u64,
                (-duration.nanos) as u32,
            ))
        }
    }
}

impl Timestamp {
    /// Normalizes the timestamp to a canonical format.
    ///
    /// Based on [`google::protobuf::util::CreateNormalized`][1].
    /// [1]: https://github.com/google/protobuf/blob/v3.3.2/src/google/protobuf/util/time_util.cc#L59-L77
    #[cfg(feature = "std")]
    pub fn normalize(&mut self) {
        // Make sure nanos is in the range.
        if self.nanos <= -NANOS_PER_SECOND || self.nanos >= NANOS_PER_SECOND {
            self.seconds += (self.nanos / NANOS_PER_SECOND) as i64;
            self.nanos %= NANOS_PER_SECOND;
        }

        // For Timestamp nanos should be in the range [0, 999999999].
        if self.nanos < 0 {
            self.seconds -= 1;
            self.nanos += NANOS_PER_SECOND;
        }

        // TODO: should this be checked?
        // debug_assert!(self.seconds >= -62_135_596_800 && self.seconds <= 253_402_300_799,
        //               "invalid timestamp: {:?}", self);
    }

    pub fn new(seconds: i64, nanos: i32) -> Self {
        let mut ts = Timestamp {
            seconds,
            nanos
        };
        ts.normalize();
        ts
    }

    pub fn to_datetime(&self) -> DateTime<Utc> {
        let dt = NaiveDateTime::from_timestamp(self.seconds, self.nanos as u32);
        DateTime::from_utc(dt, Utc)
    }

}

#[cfg(feature = "std")]
impl From<std::time::SystemTime> for Timestamp {
    fn from(system_time: std::time::SystemTime) -> Timestamp {
        let (seconds, nanos) = match system_time.duration_since(std::time::UNIX_EPOCH) {
            Ok(duration) => {
                let seconds = i64::try_from(duration.as_secs()).unwrap();
                (seconds, duration.subsec_nanos() as i32)
            }
            Err(error) => {
                let duration = error.duration();
                let seconds = i64::try_from(duration.as_secs()).unwrap();
                let nanos = duration.subsec_nanos() as i32;
                if nanos == 0 {
                    (-seconds, 0)
                } else {
                    (-seconds - 1, 1_000_000_000 - nanos)
                }
            }
        };
        Timestamp { seconds, nanos }
    }
}

#[cfg(feature = "std")]
impl From<Timestamp> for std::time::SystemTime {
    fn from(mut timestamp: Timestamp) -> std::time::SystemTime {
        timestamp.normalize();
        let system_time = if timestamp.seconds >= 0 {
            std::time::UNIX_EPOCH + time::Duration::from_secs(timestamp.seconds as u64)
        } else {
            std::time::UNIX_EPOCH - time::Duration::from_secs((-timestamp.seconds) as u64)
        };

        system_time + time::Duration::from_nanos(timestamp.nanos as u64)
    }
}

#[cfg(test)]
mod tests {
    use std::time::SystemTime;

    use proptest::prelude::*;

    use super::*;

    #[cfg(feature = "std")]
    proptest! {
        #[test]
        fn check_system_time_roundtrip(
            system_time in SystemTime::arbitrary(),
        ) {
            prop_assert_eq!(SystemTime::from(Timestamp::from(system_time)), system_time);
        }
    }
}

/// Converts chrono's `NaiveDateTime` to `Timestamp`..
impl From<NaiveDateTime> for Timestamp {
    fn from(dt: NaiveDateTime) -> Self {
        Timestamp {
            seconds: dt.timestamp(),
            nanos: dt.timestamp_subsec_nanos() as i32
        }
    }
}

/// Converts chrono's `DateTime<UTtc>` to `Timestamp`..
impl From<DateTime<Utc>> for Timestamp {
    fn from(dt: DateTime<Utc>) -> Self {
        Timestamp {
            seconds: dt.timestamp(),
            nanos: dt.timestamp_subsec_nanos() as i32
        }
    }
}

impl Serialize for Timestamp {
    fn serialize<S>(&self, serializer: S) -> Result<<S as Serializer>::Ok, <S as Serializer>::Error> where
        S: Serializer {
        let mut ts = Timestamp {
            seconds: self.seconds,
            nanos: self.nanos
        };
        ts.normalize();
        let dt = ts.to_datetime();
        serializer.serialize_str(format!("{:?}", dt).as_str())
    }
}

impl<'de> Deserialize<'de> for Timestamp  {
    fn deserialize<D>(deserializer: D) -> Result<Self, <D as Deserializer<'de>>::Error> where
        D: Deserializer<'de> {

        struct TimestampVisitor;

        impl<'de> Visitor<'de> for TimestampVisitor {
            type Value = Timestamp;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("Timestamp in RFC3339 format")
            }

            fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
                where
                    E: de::Error,
            {
                let utc: DateTime<Utc> = chrono::DateTime::from_str(value).unwrap();
                let ts = Timestamp::from(utc);
                Ok(ts)
            }
        }
        deserializer.deserialize_str(TimestampVisitor)
    }
}

/// Value Convenience Methods
///
/// A collection of methods to make working with value easier.
///

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ValueError {
    description: Cow<'static, str>,
}

impl ValueError {
    pub fn new<S>(description: S) -> Self
    where
        S: Into<Cow<'static, str>>,
    {
        ValueError {
            description: description.into(),
        }
    }
}

impl std::error::Error for ValueError {
    fn description(&self) -> &str {
        &self.description
    }
}

impl std::fmt::Display for ValueError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("failed to convert Value: ")?;
        f.write_str(&self.description)
    }
}

impl Value {
    pub fn null() -> Self {
        let kind = Some(value::Kind::NullValue(0));
        Value { kind }
    }
    pub fn number(num: f64) -> Self {
        Value::from(num)
    }
    pub fn string(s: String) -> Self {
        Value::from(s)
    }
    pub fn bool(b: bool) -> Self {
        Value::from(b)
    }
    pub fn pb_struct(m: std::collections::BTreeMap<std::string::String, Value>) -> Self {
        Value::from(m)
    }
    pub fn pb_list(l: std::vec::Vec<Value>) -> Self {
        Value::from(l)
    }
}

impl From<NullValue> for Value {
    fn from(_: NullValue) -> Self {
        Value::null()
    }
}

impl From<f64> for Value {
    fn from(num: f64) -> Self {
        let kind = Some(value::Kind::NumberValue(num));
        Value { kind }
    }
}

impl TryFrom<Value> for f64 {
    type Error = ValueError;

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        match value.kind {
            Some(value::Kind::NumberValue(num)) => Ok(num),
            Some(_other) => Err(ValueError::new(
                "Cannot convert to f64 because this is not a ValueNumber."
            )),
            _ => Err(ValueError::new(
                "Conversion to f64 failed because value is empty!",
            )),
        }
    }
}

impl From<String> for Value {
    fn from(s: String) -> Self {
        let kind = Some(value::Kind::StringValue(s));
        Value { kind }
    }
}

impl TryFrom<Value> for String {
    type Error = ValueError;

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        match value.kind {
            Some(value::Kind::StringValue(string)) => Ok(string),
            Some(_other) => Err(ValueError::new(
                "Cannot convert to String because this is not a StringValue."
            )),
            _ => Err(ValueError::new(
                "Conversion to String failed because value is empty!",
            )),
        }
    }
}

impl From<bool> for Value {
    fn from(b: bool) -> Self {
        let kind = Some(value::Kind::BoolValue(b));
        Value { kind }
    }
}

impl TryFrom<Value> for bool {
    type Error = ValueError;

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        match value.kind {
            Some(value::Kind::BoolValue(b)) => Ok(b),
            Some(_other) => Err(ValueError::new(
                "Cannot convert to bool because this is not a BoolValue."
            )),
            _ => Err(ValueError::new(
                "Conversion to bool failed because value is empty!",
            )),
        }
    }
}

impl From<std::collections::BTreeMap<std::string::String, Value>> for Value {
    fn from(fields: std::collections::BTreeMap<String, Value>) -> Self {
        let s = Struct { fields };
        let kind = Some(value::Kind::StructValue(s));
        Value { kind }
    }
}

impl TryFrom<Value> for std::collections::BTreeMap<std::string::String, Value> {
    type Error = ValueError;

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        match value.kind {
            Some(value::Kind::StructValue(s)) => Ok(s.fields),
            Some(_other) => Err(ValueError::new(
                "Cannot convert to BTreeMap<String, Value> because this is not a StructValue."
            )),
            _ => Err(ValueError::new(
                "Conversion to BTreeMap<String, Value> failed because value is empty!",
            )),
        }
    }
}

impl From<std::vec::Vec<Value>> for Value {
    fn from(values: Vec<Value>) -> Self {
        let v = ListValue { values };
        let kind = Some(value::Kind::ListValue(v));
        Value { kind }
    }
}

impl TryFrom<Value> for std::vec::Vec<Value> {
    type Error = ValueError;

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        match value.kind {
            Some(value::Kind::ListValue(list)) => Ok(list.values),
            Some(_other) => Err(ValueError::new(
                "Cannot convert to Vec<Value> because this is not a ListValue."
            )),
            _ => Err(ValueError::new(
                "Conversion to Vec<Value> failed because value is empty!",
            )),
        }
    }
}

impl Serialize for Value {
    fn serialize<S>(&self, serializer: S) -> Result<<S as Serializer>::Ok, <S as Serializer>::Error> where
        S: Serializer {
        match &self.kind {
            Some(value::Kind::NumberValue(num)) => serializer.serialize_f64(num.clone()),
            Some(value::Kind::StringValue(string)) => serializer.serialize_str(&string),
            Some(value::Kind::BoolValue(boolean)) => serializer.serialize_bool(boolean.clone()),
            Some(value::Kind::NullValue(_)) => serializer.serialize_none(),
            Some(value::Kind::ListValue(list)) => {
                let mut seq = serializer.serialize_seq(Some(list.values.len()))?;
                for e in list.clone().values {
                    seq.serialize_element(&e)?;
                }
                seq.end()
            },
            Some(value::Kind::StructValue(object)) => {
                let mut map = serializer.serialize_map(Some(object.fields.len()))?;
                for (k, v) in object.clone().fields {
                    map.serialize_entry(&k, &v)?;
                }
                map.end()
            },
            _ => serializer.serialize_none()
        }
    }
}

impl<'de> Deserialize<'de> for Value {
    fn deserialize<D>(deserializer: D) -> Result<Self, <D as Deserializer<'de>>::Error> where
        D: Deserializer<'de> {

        struct ValueVisitor;

        impl<'de> Visitor<'de> for ValueVisitor {
            type Value = crate::Value;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a prost_types::Value struct")
            }

            fn visit_bool<E>(self, value: bool) -> Result<Self::Value, E>
                where
                    E: de::Error,
            {
                Ok(Value::from(value))
            }

            fn visit_i64<E>(self, value: i64) -> Result<Self::Value, E>
                where
                    E: de::Error,
            {
                Ok(Value::from(value as f64))
            }

            fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E>
                where
                    E: de::Error,
            {
                Ok(Value::from(value as f64))
            }

            fn visit_f64<E>(self, value: f64) -> Result<Self::Value, E>
                where
                    E: de::Error,
            {
                Ok(Value::from(value))
            }

            fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
                where
                    E: de::Error,
            {
                Ok(Value::from(String::from(value)))
            }

            fn visit_string<E>(self, value: String) -> Result<Self::Value, E>
                where
                    E: de::Error,
            {
                Ok(Value::from(value))
            }

            fn visit_none<E>(self) -> Result<Self::Value, E>
                where
                    E: de::Error,
            {
                Ok(Value::null())
            }

            fn visit_unit<E>(self) -> Result<Self::Value, E>
                where
                    E: de::Error,
            {
                Ok(Value::null())
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
                where
                    A: SeqAccess<'de>,
            {
                let mut values: Vec<Value> = Vec::new();
                while let Some(el) = seq.next_element()? {
                    values.push(el)
                }
                Ok(Value::from(values))
            }

            fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
                where
                    A: MapAccess<'de>,
            {
                let mut fields: std::collections::BTreeMap<String, Value> = BTreeMap::new();
                while let Some((key, value)) = map.next_entry()? {
                    fields.insert(key, value);
                }
                Ok(Value::from(fields))
            }

        }
        deserializer.deserialize_any(ValueVisitor)
    }
}

/// Any Convenience Methods
///
/// Pack and unpack for Any value
///

use prost::MessageSerde;
use serde_json::json;

impl Any {
    // A type_url can take the format of `type.googleapis.com/package_name.struct_name`
    pub fn pack<T>(message: T) -> Self
    where
        T: prost::Message + prost::MessageMeta + Default
    {
        let type_url= prost::MessageMeta::type_url(&message).to_string();
        // Serialize the message into a value
        let mut buf = Vec::new();
        buf.reserve(message.encoded_len());
        message.encode(&mut buf).unwrap();
        Any {
            type_url,
            value: buf,
        }
    }

    pub fn unpack_as<T: prost::Message>(self, mut target: T) -> Result<T, prost::DecodeError> {
        let mut cursor = std::io::Cursor::new(self.value.as_slice());
        target.merge(&mut cursor).map(|_| target)
    }

    pub fn unpack(self) -> Result<Box<dyn prost::MessageSerde>, prost::DecodeError> {
        let type_url = self.type_url.clone();
        let empty = json!({
            "@type": &type_url,
            "value": {}
        });
        let template: Box<dyn MessageSerde> = serde_json::from_value(empty)
            .map_err(|error| {
                let description = format!(
                    "Failed to deserialize {}. Make sure it implements Serialize and Deserialize. Error reported: {}",
                    type_url,
                    error.to_string()
                );
                prost::DecodeError::new(description)
            })?;
        template.new_instance(self.value.clone())
    }
}

impl Serialize for Any {
    fn serialize<S>(&self, serializer: S) -> Result<<S as Serializer>::Ok, <S as Serializer>::Error> where
        S: Serializer {
        match self.clone().unpack() {
            Ok(result) => {
                serde::ser::Serialize::serialize(result.as_ref(), serializer)
            },
            Err(_) => {
                let mut state = serializer.serialize_struct("Any", 3)?;
                state.serialize_field("@type", &self.type_url)?;
                state.serialize_field("value", &self.value)?;
                state.end()
            }
        }
    }
}

impl<'de> Deserialize<'de> for Any  {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error> where
        D: Deserializer<'de>,
    {
        let erased: Box<dyn MessageSerde> = serde::de::Deserialize::deserialize(deserializer).unwrap();
        let type_url = erased.type_url().to_string();
        let value = erased.encoded();
        Ok(
            Any {
                type_url,
                value
            }
        )
    }
}
