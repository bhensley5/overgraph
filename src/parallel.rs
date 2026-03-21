use std::sync::OnceLock;

fn engine_cpu_pool() -> &'static rayon::ThreadPool {
    static ENGINE_CPU_POOL: OnceLock<rayon::ThreadPool> = OnceLock::new();

    ENGINE_CPU_POOL.get_or_init(|| {
        let threads = std::thread::available_parallelism()
            .map(std::num::NonZeroUsize::get)
            .unwrap_or(1);
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
