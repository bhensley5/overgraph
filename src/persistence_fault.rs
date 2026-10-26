//! Native-only named faults around actual persistence syscalls.
use crate::error::EngineError;
use std::cell::Cell;
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Boundary {
    BeforeTmp,
    TmpCreate,
    TmpWrite,
    TmpSync,
    PrevRemove,
    CurrentToPrev,
    TmpToCurrent,
    DirSync,
    WalRetire,
    PlannerStatsPrepare,
    ComponentCreate,
    ComponentWrite,
    ComponentSync,
    ComponentRename,
    ComponentDirSync,
}
thread_local! { static FAULT: Cell<Option<(Boundary, bool)>> = const { Cell::new(None) }; }
pub(crate) fn set(boundary: Boundary, abort: bool) {
    FAULT.with(|f| {
        assert!(f.get().is_none());
        f.set(Some((boundary, abort)));
    });
}
pub(crate) fn check(boundary: Boundary) -> Result<(), EngineError> {
    FAULT.with(|f| {
        if let Some((at, abort)) = f.get() {
            if at == boundary {
                f.set(None);
                if abort {
                    eprintln!("p45a1 named crash: {boundary:?}");
                    std::process::abort();
                }
                return Err(std::io::Error::other(format!(
                    "p45a1 named persistence fault: {boundary:?}"
                ))
                .into());
            }
        }
        Ok(())
    })
}
