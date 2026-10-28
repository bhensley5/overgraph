use std::sync::OnceLock;

/// Freeze the existing pool's configured width without creating its workers.
/// Admission and the later pool constructor must use the same choice even if
/// host affinity/available parallelism changes between preflight and execution.
pub(crate) fn engine_cpu_threads() -> usize {
    static THREADS: OnceLock<usize> = OnceLock::new();
    *THREADS.get_or_init(|| {
        std::thread::available_parallelism()
            .map(std::num::NonZeroUsize::get)
            .unwrap_or(1)
    })
}

fn engine_cpu_pool() -> &'static rayon::ThreadPool {
    static ENGINE_CPU_POOL: OnceLock<rayon::ThreadPool> = OnceLock::new();

    ENGINE_CPU_POOL.get_or_init(|| {
        let threads = engine_cpu_threads();
        rayon::ThreadPoolBuilder::new()
            .num_threads(threads)
            .thread_name(|index| format!("overgraph-cpu-{index}"))
            .build()
            .expect("overgraph shared Rayon pool should build")
    })
}

pub(crate) fn engine_cpu_install<F, R>(f: F) -> R
where
    F: FnOnce() -> R + Send,
    R: Send,
{
    engine_cpu_pool().install(f)
}

pub(crate) fn engine_cpu_join<A, B, RA, RB>(left: A, right: B) -> (RA, RB)
where
    A: FnOnce() -> RA + Send,
    B: FnOnce() -> RB + Send,
    RA: Send,
    RB: Send,
{
    engine_cpu_pool().install(|| rayon::join(left, right))
}

pub(crate) fn engine_cpu_try_join<A, B, RA, RB, E>(left: A, right: B) -> Result<(RA, RB), E>
where
    A: FnOnce() -> Result<RA, E> + Send,
    B: FnOnce() -> Result<RB, E> + Send,
    RA: Send,
    RB: Send,
    E: Send,
{
    let (left_result, right_result) = engine_cpu_join(left, right);
    Ok((left_result?, right_result?))
}
