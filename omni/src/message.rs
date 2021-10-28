pub mod error;
pub mod request;
pub mod response;

pub use error::OmniError;
pub use request::RequestMessage;
pub use request::RequestMessageBuilder;
pub use response::ResponseMessage;
pub use response::ResponseMessageBuilder;

use crate::Identity;
use minicose::exports::ciborium::value::Value;
use minicose::{
    AlgorithmicCurve, Algorithms, CoseKey, CoseKeySet, CoseSign1, CoseSign1Builder, Ed25519CoseKey,
    Ed25519CoseKeyBuilder, ProtectedHeaders, ProtectedHeadersBuilder,
};
use ring::signature::KeyPair;
use std::convert::TryFrom;

pub fn decode_request_from_cose_sign1(
    sign1: CoseSign1,
    to: Option<Identity>,
) -> Result<RequestMessage, OmniError> {
    let request = CoseSign1RequestMessage { sign1 };
    let from_id = request
        .verify()
        .map_err(|_| OmniError::could_not_verify_signature())?;

    let payload = request.sign1.payload.ok_or(OmniError::empty_envelope())?;
    let message =
        RequestMessage::from_bytes(&payload).map_err(|_| OmniError::internal_server_error())?;

    // Check the `from` field.
    if from_id != message.from.unwrap_or_default() {
        return Err(OmniError::invalid_from_identity());
    }

    // Check the `to` field to make sure we have the right one.
    if let Some(ref to_id) = to {
        if to_id != &message.to {
            return Err(OmniError::unknown_destination(
                message.to.to_string(),
                to_id.to_string(),
            ));
        }
    }

    Ok(message)
}

pub fn decode_response_from_cose_sign1(
    sign1: CoseSign1,
    to: Option<Identity>,
) -> Result<ResponseMessage, String> {
    let request = CoseSign1RequestMessage { sign1 };
    let from_id = request.verify()?;

    let payload = request
        .sign1
        .payload
        .ok_or("Envelope does not have payload.".to_string())?;
    let message = ResponseMessage::from_bytes(&payload)?;

    // Check the `from` field.
    if from_id != message.from {
        return Err("The message's from field does not match the envelope.".to_string());
    }

    // Check the `to` field to make sure we have the right one.
    if let Some(to_id) = to {
        if to_id != message.to.unwrap_or_default() {
            return Err("The message's to field is not for this server.".to_string());
        }
    }

    Ok(message)
}

fn encode_cose_sign1_from_payload(
    payload: Vec<u8>,
    id: Identity,
    keypair: &Option<ring::signature::Ed25519KeyPair>,
) -> Result<CoseSign1, String> {
    let maybe_cose_key: Option<CoseKey> = keypair.as_ref().map(|kp| {
        let x = kp.public_key().as_ref().to_vec();
        Ed25519CoseKeyBuilder::default()
            .x(x)
            .kid(id.to_vec())
            .build()
            .unwrap()
            .into()
    });

    if !id.matches_key(&maybe_cose_key) {
        return Err("Identity did not match keypair.".to_string());
    }

    let mut protected: ProtectedHeaders = ProtectedHeadersBuilder::default()
        .alg(Algorithms::EdDSA(AlgorithmicCurve::Ed25519))
        .kid(id.to_vec())
        .content_type("application/cbor".to_string())
        .build()
        .unwrap();

    // Add the keyset to the headers.
    if let Some(cose_key) = maybe_cose_key {
        let mut keyset = CoseKeySet::default();
        keyset.insert(cose_key);

        protected.custom_headers.insert(
            Value::from("keyset"),
            Value::from(keyset.to_bytes().map_err(|e| e.to_string()).unwrap()),
        );
    }

    let mut cose: CoseSign1 = CoseSign1Builder::default()
        .protected(protected)
        .payload(payload)
        .build()
        .unwrap();

    if let Some(ref kp) = keypair {
        cose.sign_with(|bytes| Ok(kp.sign(bytes).as_ref().to_vec()))
            .map_err(|e| e.to_string())?;
    }
    Ok(cose)
}

pub fn encode_cose_sign1_from_response(
    response: ResponseMessage,
    id: Identity,
    keypair: &Option<ring::signature::Ed25519KeyPair>,
) -> Result<CoseSign1, String> {
    encode_cose_sign1_from_payload(response.to_bytes().unwrap(), id, keypair)
}

pub fn encode_cose_sign1_from_request(
    request: RequestMessage,
    id: Identity,
    keypair: &Option<ring::signature::Ed25519KeyPair>,
) -> Result<CoseSign1, String> {
    encode_cose_sign1_from_payload(request.to_bytes().unwrap(), id, keypair)
}

/// Provide utility functions surrounding request and response messages.
#[derive(Clone, Debug, Default)]
pub(crate) struct CoseSign1RequestMessage {
    pub sign1: CoseSign1,
}

impl CoseSign1RequestMessage {
    pub fn get_keyset(&self) -> Option<CoseKeySet> {
        let keyset = self.sign1.protected.get("keyset".to_string())?;

        if let Value::Bytes(ref bytes) = keyset {
            CoseKeySet::from_bytes(bytes).ok()
        } else {
            None
        }
    }

    pub fn get_public_key_for_identity(
        &self,
        id: &Identity,
    ) -> Option<ring::signature::UnparsedPublicKey<Vec<u8>>> {
        // Verify the keybytes matches the identity.
        if id.is_anonymous() {
            return None;
        }
        // Find the key_bytes.
        let cose_key = self.get_keyset()?.get_kid(&id.to_vec()).cloned()?;
        let ed25519_key = Ed25519CoseKey::try_from(cose_key.clone()).ok()?;
        let key_bytes = ed25519_key.x?;

        if id.is_public_key() {
            let other = Identity::public_key(&cose_key);
            if other.eq(id) {
                Some(ring::signature::UnparsedPublicKey::new(
                    &ring::signature::ED25519,
                    key_bytes,
                ))
            } else {
                None
            }
        } else if id.is_addressable() {
            if Identity::addressable(&cose_key).eq(id) {
                // Some(cosekey_to_ring_key(key_bytes))
                Some(ring::signature::UnparsedPublicKey::new(
                    &ring::signature::ED25519,
                    key_bytes,
                ))
            } else {
                None
            }
        } else {
            None
        }
    }

    pub fn verify(&self) -> Result<Identity, String> {
        if let Some(ref kid) = self.sign1.protected.kid {
            if let Ok(id) = Identity::from_bytes(kid) {
                if id.is_anonymous() {
                    return Ok(id);
                }

                self.get_public_key_for_identity(&id)
                    .ok_or("Could not find a public key in the envelope".to_string())
                    .and_then(|key| {
                        self.sign1
                            .verify_with(|content, sig| key.verify(content, sig).is_ok())
                            .map_err(|e| e.to_string())
                    })
                    .and_then(|valid| {
                        if !valid {
                            Err("Envelope does not verify.".to_string())
                        } else {
                            Ok(id)
                        }
                    })
            } else {
                Err("Invalid (not an OMNI identity) key ID".to_string())
            }
        } else {
            Err("Missing key ID".to_string())
        }
    }
}
