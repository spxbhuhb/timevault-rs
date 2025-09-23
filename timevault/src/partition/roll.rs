use crate::errors::Result;
use crate::partition::PartitionHandle;

pub fn force_roll(_h: &PartitionHandle) -> Result<()> {
    // Minimal placeholder: real rolling handled implicitly by chunk creation
    Ok(())
}
