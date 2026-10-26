//! Connector metadata conversion. All JS allocation happens on the JS thread.
use super::*;

pub(super) const MAX_SAFE_INTEGER: u64 = (1u64 << 53) - 1;
pub struct DiagnosticAmount(pub u64);
impl TypeName for DiagnosticAmount {
    fn type_name() -> &'static str {
        "number | bigint"
    }
    fn value_type() -> napi::ValueType {
        napi::ValueType::Unknown
    }
}
impl ToNapiValue for DiagnosticAmount {
    unsafe fn to_napi_value(
        env: napi::sys::napi_env,
        value: Self,
    ) -> Result<napi::sys::napi_value> {
        if value.0 <= MAX_SAFE_INTEGER {
            unsafe { f64::to_napi_value(env, value.0 as f64) }
        } else {
            unsafe {
                BigInt::to_napi_value(
                    env,
                    BigInt {
                        sign_bit: false,
                        words: vec![value.0],
                    },
                )
            }
        }
    }
}
pub(super) fn engine_error(env: Env, error: EngineError) -> napi::Error {
    let Some(code) = error.code() else {
        return napi::Error::from_reason(error.to_string());
    };
    let converted = (|| -> Result<napi::Error> {
        let value =
            napi::JsError::from(napi::Error::from_reason(error.to_string())).into_unknown(env);
        let mut object = Object::from_raw(env.raw(), value.raw());
        object.set_named_property("code", code)?;
        if let EngineError::ResourceLimit {
            resource,
            requested,
            limit,
            retryable,
        } = error
        {
            object.set_named_property("resource", resource)?;
            object.set_named_property("requested", DiagnosticAmount(requested))?;
            object.set_named_property("limit", DiagnosticAmount(limit))?;
            object.set_named_property("retryable", retryable)?;
        }
        Ok(napi::Error::from(value))
    })();
    converted.unwrap_or_else(|e| e)
}

/// Input-only conversion on the JS thread. Retain owned limits, never JS handles.
/// serde_json::Value cannot be used here: napi converts small BigInts to numbers.
pub struct ConcurrencyInput(overgraph::ConcurrencyOptions);
impl TypeName for ConcurrencyInput {
    fn type_name() -> &'static str {
        "ConcurrencyOptions"
    }
    fn value_type() -> napi::ValueType {
        napi::ValueType::Object
    }
}
impl ValidateNapiValue for ConcurrencyInput {}
impl FromNapiValue for ConcurrencyInput {
    unsafe fn from_napi_value(
        env: napi::sys::napi_env,
        value: napi::sys::napi_value,
    ) -> Result<Self> {
        let value = unsafe { Unknown::from_napi_value(env, value)? };
        if value.get_type()? != napi::ValueType::Object || value.is_array()? {
            return Err(napi::Error::from_reason("concurrency must be an object"));
        }
        let object = unsafe { value.cast::<Object<'_>>()? };
        let mut options = overgraph::ConcurrencyOptions::default();
        for key in Object::keys(&object)? {
            if !matches!(
                key.as_str(),
                "maxBatchOperations"
                    | "maxBatchBytes"
                    | "generationOperationLimit"
                    | "generationWalBytes"
                    | "admissionTimeoutMs"
            ) {
                return Err(napi::Error::from_reason(format!(
                    "unknown concurrency option: {key}"
                )));
            }
            let invalid = || {
                napi::Error::from_reason(format!(
                    "concurrency.{key} must be a nonnegative safe integer Number"
                ))
            };
            let value: Unknown<'_> = object.get_named_property(&key)?;
            if value.get_type()? != napi::ValueType::Number {
                return Err(invalid());
            }
            let number = unsafe { value.cast::<f64>()? };
            if !number.is_finite()
                || number < 0.0
                || number.fract() != 0.0
                || number > MAX_SAFE_INTEGER as f64
            {
                return Err(invalid());
            }
            let number = number as u64;
            let size = || {
                usize::try_from(number).map_err(|_| {
                    napi::Error::from_reason(format!("concurrency.{key} overflows usize"))
                })
            };
            match key.as_str() {
                "maxBatchOperations" => options.max_batch_operations = size()?,
                "maxBatchBytes" => options.max_batch_bytes = size()?,
                "generationOperationLimit" => options.generation_operation_limit = size()?,
                "generationWalBytes" => options.generation_wal_bytes = size()?,
                "admissionTimeoutMs" => options.admission_timeout_ms = number,
                _ => unreachable!("validated option name"),
            }
        }
        options
            .validate()
            .map_err(|e| napi::Error::from_reason(e.to_string()))?;
        Ok(Self(options))
    }
}
#[cfg(feature = "production-concurrency")]
pub(super) fn options(value: Option<&ConcurrencyInput>) -> overgraph::ConcurrencyOptions {
    value.map(|value| value.0.clone()).unwrap_or_default()
}
pub struct BindingStats(pub overgraph::ReclamationStats);
impl TypeName for BindingStats {
    fn type_name() -> &'static str {
        "ReclamationStats"
    }
    fn value_type() -> napi::ValueType {
        napi::ValueType::Object
    }
}
fn histogram(env: Env, value: overgraph::ReclamationHistogram) -> Result<Object<'static>> {
    let mut object = Object::new(&env)?;
    object.set_named_property(
        "buckets",
        value
            .buckets
            .into_iter()
            .map(DiagnosticAmount)
            .collect::<Vec<_>>(),
    )?;
    object.set_named_property("samples", DiagnosticAmount(value.samples))?;
    object.set_named_property("maxUs", value.max_us.map(DiagnosticAmount))?;
    Ok(Object::from_raw(env.raw(), object.raw()))
}
impl ToNapiValue for BindingStats {
    unsafe fn to_napi_value(
        raw: napi::sys::napi_env,
        value: Self,
    ) -> Result<napi::sys::napi_value> {
        let env = Env::from_raw(raw);
        let mut object = Object::new(&env)?;
        let s = value.0;
        macro_rules! amount {
            ($key:literal,$field:ident) => {
                object.set_named_property($key, DiagnosticAmount(s.$field))?;
            };
        }
        amount!("registeredObjects", registered_objects);
        amount!("retiredObjects", retired_objects);
        amount!("droppingObjects", dropping_objects);
        amount!("activeGenerations", active_generations);
        amount!("frozenGenerations", frozen_generations);
        amount!("leasedParticipants", leased_participants);
        amount!("participantLimit", participant_limit);
        amount!("processParticipants", process_participants);
        amount!("activeSessions", active_sessions);
        amount!("drainingSessions", draining_sessions);
        let state = match s.close_state {
            overgraph::ReclamationCloseState::Open => "open",
            overgraph::ReclamationCloseState::Draining => "draining",
            overgraph::ReclamationCloseState::Drained => "drained",
            _ => return Err(napi::Error::from_reason("unknown close state")),
        };
        object.set_named_property("closeState", state)?;
        object.set_named_property(
            "oldestRetirementAgeUs",
            s.oldest_retirement_age_us.map(DiagnosticAmount),
        )?;
        object.set_named_property("scanLagUs", s.scan_lag_us.map(DiagnosticAmount))?;
        object.set_named_property(
            "releaseToEligibilityUs",
            histogram(env, s.release_to_eligibility_us)?,
        )?;
        object.set_named_property(
            "eligibilityToDropUs",
            histogram(env, s.eligibility_to_drop_us)?,
        )?;
        object.set_named_property("workerWallUs", s.worker_wall_us.map(DiagnosticAmount))?;
        object.set_named_property("workerCpuUs", s.worker_cpu_us.map(DiagnosticAmount))?;
        Ok(object.raw())
    }
}

#[cfg(feature = "production-concurrency")]
#[derive(Clone)]
pub(super) struct PendingClose(Arc<CloseDelivery>);
#[cfg(feature = "production-concurrency")]
struct CloseDelivery {
    promise: Mutex<Option<ObjectRef>>,
    deferred: usize,
    delivered: std::sync::atomic::AtomicBool,
}
#[cfg(feature = "production-concurrency")]
impl PendingClose {
    #[cfg(feature = "binding-test-hooks")]
    pub fn has_reference(&self) -> bool {
        self.0.promise.lock().unwrap().is_some()
    }
    pub fn is_delivered(&self) -> bool {
        self.0.delivered.load(std::sync::atomic::Ordering::Acquire)
    }
    pub fn get_promise(&self, env: Env) -> Result<Object<'static>> {
        let guard = self.0.promise.lock().unwrap();
        let value = guard
            .as_ref()
            .expect("pending Promise reference")
            .get_value(&env)?;
        Ok(Object::from_raw(env.raw(), value.raw()))
    }
    pub fn release(&self, env: Env) -> Result<()> {
        let reference = self.0.promise.lock().unwrap().take();
        if let Some(reference) = reference {
            reference.unref(&env)?;
        }
        Ok(())
    }
    fn settle(&self, env: Env, result: std::result::Result<(), EngineError>) -> Result<()> {
        if self
            .0
            .delivered
            .swap(true, std::sync::atomic::Ordering::AcqRel)
        {
            return self.release(env);
        }
        let raw = env.raw();
        let status = match result {
            Ok(()) => {
                let value = unsafe { <()>::to_napi_value(raw, ()) }?;
                unsafe {
                    napi::sys::napi_resolve_deferred(
                        raw,
                        self.0.deferred as napi::sys::napi_deferred,
                        value,
                    )
                }
            }
            Err(error) => {
                let value = napi::JsError::from(engine_error(env, error)).into_unknown(env);
                unsafe {
                    napi::sys::napi_reject_deferred(
                        raw,
                        self.0.deferred as napi::sys::napi_deferred,
                        value.raw(),
                    )
                }
            }
        };
        // A completed wrapper must not root a Promise or arbitrary caller data
        // attached to it. Only the fixed completion metadata survives delivery.
        self.release(env)?;
        napi::check_status!(status)?;
        Ok(())
    }
    pub fn discard(&self, env: Env) -> Result<()> {
        // This Promise was never returned: free the native deferred without an
        // unhandled rejection, then throw the actual request error to the caller.
        self.settle(env, Ok(()))
    }
}
#[cfg(feature = "production-concurrency")]
pub(super) fn promise(
    env: Env,
    completion: overgraph::BindingCloseHandle,
) -> Result<(Object<'static>, PendingClose)> {
    let mut deferred = std::ptr::null_mut();
    let mut raw = std::ptr::null_mut();
    napi::check_status!(unsafe {
        napi::sys::napi_create_promise(env.raw(), &mut deferred, &mut raw)
    })?;
    let object = Object::from_raw(env.raw(), raw);
    let state = PendingClose(Arc::new(CloseDelivery {
        promise: Mutex::new(None),
        deferred: deferred as usize,
        delivered: std::sync::atomic::AtomicBool::new(false),
    }));
    let setup = (|| -> Result<()> {
        *state.0.promise.lock().unwrap() = Some(object.create_ref::<true>()?);
        let mark = state.clone();
        let observed = completion.clone();
        let callback =
            env.create_function_from_closure::<(), (), _>("overgraphNativeClose", move |ctx| {
                let result = observed
                    .result()
                    .expect("native completion precedes notification");
                mark.settle(*ctx.env, result)
            })?;
        let tsfn = callback
            .build_threadsafe_function::<()>()
            .callee_handled::<false>()
            .max_queue_size::<1>()
            .build_callback(|_| Ok(()))?;
        completion
            .register_notification(move || {
                let _ = tsfn.call((), ThreadsafeFunctionCallMode::NonBlocking);
            })
            .map_err(|e| engine_error(env, e))?;
        Ok(())
    })();
    if let Err(error) = setup {
        state.discard(env)?;
        return Err(error);
    }
    Ok((object, state))
}
#[cfg(feature = "production-concurrency")]
pub(super) fn fulfilled(env: Env) -> Result<Object<'static>> {
    let mut deferred = std::ptr::null_mut();
    let mut raw = std::ptr::null_mut();
    napi::check_status!(unsafe {
        napi::sys::napi_create_promise(env.raw(), &mut deferred, &mut raw)
    })?;
    let value = unsafe { <()>::to_napi_value(env.raw(), ()) }?;
    napi::check_status!(unsafe { napi::sys::napi_resolve_deferred(env.raw(), deferred, value) })?;
    Ok(Object::from_raw(env.raw(), raw))
}
