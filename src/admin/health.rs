#[derive(Debug, Default, Clone)]
pub struct HealthReport {
    pub ok: bool,
    pub notes: Vec<String>,
}
