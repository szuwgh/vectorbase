use chrono::Local;

pub struct Time;

impl Time {
    pub fn now() -> i64 {
        Local::now().timestamp()
    }
}
