use minicbor::data::Type;
use minicbor::encode::{Error, Write};
use minicbor::{Decode, Decoder, Encode, Encoder};
use std::collections::BTreeMap;
use std::fmt::{Display, Formatter};
use std::iter::FromIterator;

pub const RESERVED_OMNI_ERROR_CODE: u32 = 10000;

macro_rules! omni_error {
    {
        $(
            $v: literal: $name: ident $(as $snake_name: ident ( $($arg: ident),* ))? => $description: literal,
        )*
    } => {
        #[derive(Copy, Clone, Debug)]
        pub enum OmniErrorCode {
            $( $name, )*
            ApplicationSpecific(u32),
        }

        impl OmniErrorCode {
            #[inline]
            pub fn message(&self) -> Option<&'static str> {
                match self {
                    $( OmniErrorCode::$name => Some($description), )*
                    _ => None,
                }
            }
        }

        impl From<u32> for OmniErrorCode {
            fn from(v: u32) -> Self {
                match v {
                    $(
                        $v => Self::$name,
                    )*
                    x if x >= RESERVED_OMNI_ERROR_CODE => Self::ApplicationSpecific(x),
                    _ => Self::Unknown,
                }
            }
        }
        impl Into<u32> for OmniErrorCode {
            fn into(self) -> u32 {
                match self {
                    $(
                        Self::$name => $v,
                    )*
                    Self::ApplicationSpecific(x) => x,
                }
            }
        }

        #[derive(Clone, Debug)]
        pub struct OmniError {
            pub code: OmniErrorCode,
            pub message: Option<String>,
            pub fields: BTreeMap<String, String>,
        }

        impl OmniError {
            $($(
                #[doc = $description]
                pub fn $snake_name( $($arg: String,)* ) -> Self {
                    Self {
                        code: OmniErrorCode::$name,
                        message: None,
                        fields: BTreeMap::from_iter(vec![
                            $( (stringify!($arg).to_string(), $arg) ),*
                        ]),
                    }
                }
            )?)*
        }
    }
}

omni_error! {
    // Range 0-999 is for unexpected or transport errors.
       0: Unknown as unknown()
            => "Unknown error.",
       1: MessageTooLong as message_too_long(max)
            => "Message is too long. Max allowed size is {max} bytes.",

    // 1000-1999 is for request errors.
    1000: InvalidMethodName as invalid_method_name(method)
            => r#"Invalid method name: "{method}"."#,
    1001: InvalidFromIdentity as invalid_from_identity()
            => "The identity of the from field is invalid or unexpected.",
    1002: CouldNotVerifySignature as could_not_verify_signature()
            => "Signature does not match the public key.",
    1003: UnknownDestination as unknown_destination(to, this)
            => "Unknown destination for message.\nThis is \"{this}\", message was for \"{to}\".",
    1004: EmptyEnvelope as empty_envelope()
            => "An envelope must contain a payload.",

    // 2000-2999 is for server errors.
    2000: InternalServerError as internal_server_error()
            => "An internal server error happened.",

    // 10000+ are reserved for application codes and are defined separately.
}

impl OmniErrorCode {
    #[inline]
    pub fn is_application_specific(&self) -> bool {
        matches!(self, OmniErrorCode::ApplicationSpecific(x) if x >= &RESERVED_OMNI_ERROR_CODE)
    }

    #[inline]
    pub fn message_of(code: u32) -> Option<&'static str> {
        OmniErrorCode::from(code).message()
    }
}

impl OmniError {
    #[inline]
    pub fn is_application_specific(&self) -> bool {
        self.code.is_application_specific()
    }
}

impl Default for OmniErrorCode {
    #[inline]
    fn default() -> Self {
        OmniErrorCode::Unknown
    }
}

impl Default for OmniError {
    #[inline]
    fn default() -> Self {
        OmniError::unknown()
    }
}

impl Display for OmniError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let message = self
            .message
            .as_ref()
            .map(|s| s.as_str())
            .unwrap_or_else(|| self.code.message().unwrap_or("Invalid error code."));

        let re = regex::Regex::new(r"\{\{|\}\}|\{[^\}]*\}").unwrap();
        let mut current = 0;

        for mat in re.find_iter(message) {
            let std::ops::Range { start, end } = mat.range();
            f.write_str(&message[current..start])?;
            current = end;

            let s = mat.as_str();
            if s == "{{" {
                f.write_str("{")?;
            } else if s == "}}" {
                f.write_str("}")?;
            } else {
                let field = &message[start + 1..end - 1];
                f.write_str(self.fields.get(field).unwrap_or(&"".to_string()).as_str())?;
            }
        }
        f.write_str(&message[current..])
    }
}

impl std::error::Error for OmniError {}

impl Encode for OmniError {
    fn encode<W: Write>(&self, e: &mut Encoder<W>) -> Result<(), Error<W::Error>> {
        match (&self.message, self.fields.is_empty()) {
            (Some(msg), true) => e.array(2)?.u32(self.code.into())?.str(msg.as_str())?,
            (Some(msg), false) => e
                .array(3)?
                .u32(self.code.into())?
                .str(msg.as_str())?
                .encode(&self.fields)?,
            (None, true) => e.array(1)?.u32(self.code.into())?,
            (None, false) => e.array(2)?.u32(self.code.into())?.encode(&self.fields)?,
        };
        Ok(())
    }
}

impl<'b> Decode<'b> for OmniError {
    fn decode(d: &mut Decoder<'b>) -> Result<Self, minicbor::decode::Error> {
        d.array()?;
        let code: OmniErrorCode = d.u32()?.into();

        if code.is_application_specific() {
            Ok(Self {
                code,
                message: Some(d.str()?.to_string()),
                fields: match d.datatype() {
                    Ok(Type::Map) => d.decode()?,
                    _ => BTreeMap::new(),
                },
            })
        } else {
            Ok(Self {
                code,
                message: None,
                fields: match d.datatype() {
                    Ok(Type::Map) => d.decode()?,
                    _ => BTreeMap::new(),
                },
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::message::error::OmniErrorCode as ErrorCode;
    use crate::OmniError;
    use std::collections::BTreeMap;

    #[test]
    fn works() {
        let mut fields = BTreeMap::new();
        fields.insert("0".to_string(), "ZERO".to_string());
        fields.insert("1".to_string(), "ONE".to_string());
        fields.insert("2".to_string(), "TWO".to_string());

        let e = OmniError {
            code: ErrorCode::Unknown,
            message: Some("Hello {0} and {2}.".to_string()),
            fields,
        };

        assert_eq!(format!("{}", e), "Hello ZERO and TWO.");
    }

    #[test]
    fn works_with_only_replacement() {
        let mut fields = BTreeMap::new();
        fields.insert("0".to_string(), "ZERO".to_string());
        fields.insert("1".to_string(), "ONE".to_string());
        fields.insert("2".to_string(), "TWO".to_string());

        let e = OmniError {
            code: ErrorCode::Unknown,
            message: Some("{2}".to_string()),
            fields,
        };

        assert_eq!(format!("{}", e), "TWO");
    }

    #[test]
    fn works_for_others() {
        let mut fields = BTreeMap::new();
        fields.insert("0".to_string(), "ZERO".to_string());
        fields.insert("1".to_string(), "ONE".to_string());
        fields.insert("2".to_string(), "TWO".to_string());

        let e = OmniError {
            code: ErrorCode::Unknown,
            message: Some("@{a}{b}{c}.".to_string()),
            fields,
        };

        assert_eq!(format!("{}", e), "@.");
    }

    #[test]
    fn supports_double_brackets() {
        let mut fields = BTreeMap::new();
        fields.insert("0".to_string(), "ZERO".to_string());
        fields.insert("1".to_string(), "ONE".to_string());
        fields.insert("2".to_string(), "TWO".to_string());

        let e = OmniError {
            code: ErrorCode::Unknown,
            message: Some("/{{}}{{{0}}}{{{a}}}{b}}}{{{2}.".to_string()),
            fields,
        };

        assert_eq!(format!("{}", e), "/{}{ZERO}{}}{TWO.");
    }
}
