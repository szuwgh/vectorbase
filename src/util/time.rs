use quanta::Clock;
use quanta::Instant;
pub struct Time;

impl Time {
    pub fn now() -> u64 {
        Clock::default().raw()
        //Local::now().timestamp()
    }
}
