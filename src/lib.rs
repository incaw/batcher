use core::ops::Fn;
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use std::time::{SystemTime, UNIX_EPOCH};

fn sleep_ms(ms: u64) {
    thread::sleep(Duration::from_millis(ms));
}

pub struct Batch< T: 'static> {
    pub size: usize,
    pub functions: Vec<Arc<dyn Fn() -> T + Send + Sync>>,
    pub stats: bool,
    pub batch_wait_dur: u64,
}

impl<T: 'static> Batch< T> {
    pub fn new() -> Self {
        Self {
            size: 0,
            functions: Vec::new(),
            stats: false,
            batch_wait_dur: 0
        }
    }

    pub fn set_size(&mut self, s: usize) {
        self.size = s;
    }

    pub fn set_stats(&mut self, s: bool) {
        self.stats = s;
    }

    pub fn set_batch_wait_dur(&mut self, s: u64) {
        self.batch_wait_dur = s;
    }

    pub fn add_func(&mut self, func: impl Fn() -> T + Send + Sync + 'static) {
        self.functions.push(Arc::new(func));
    }

    pub fn length(&self) -> usize {
        return self.functions.len();
    }

    pub fn itr_fn(&self, arr: Vec<Arc<dyn Fn() -> T + Send + Sync>>) {
        let mut handles = Vec::new();
        for i in arr {
           let handle = thread::spawn(move || {
           i();
        }); 
           handles.push(handle);
        }
        for i in handles {
            i.join().unwrap()
        }
    }

    pub fn execute_linear(&self) {
        let start = self.get_time_in_ms();
        self.itr_fn(self.functions.clone());
        if self.stats {
            println!(
                "finished linear batch in {:?} ms",
                self.get_time_in_ms() - start
            );
        }
    }

    pub fn execute(&self) {
        let start = self.get_time_in_ms();

        let mut prev_start = 0;

        while self.functions.len() - prev_start > self.size {

            let con_arr = self.functions[prev_start..prev_start + self.size].to_vec();
            self.itr_fn(con_arr);

            prev_start += self.size;

            if self.batch_wait_dur > 0 {
                sleep_ms(self.batch_wait_dur);
            }
        }

        let con_arr = self.functions[prev_start..].to_vec();
        self.itr_fn(con_arr);

        if self.stats {
            println!(
                "finished batching operation in {:?} ms",
                self.get_time_in_ms() - start
            );
        }
    }

    pub fn get_time_in_ms(&self) -> u64 {
        let start_time = SystemTime::now();
        let since_the_epoch = start_time.duration_since(UNIX_EPOCH).unwrap();
        let time_in_ms = since_the_epoch.as_millis() as u64;
        time_in_ms
    }
}

 #[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn batch_size() {

        fn func() {}

        let mut batch = Batch::new();

        for _ in 0..50 {
            batch.add_func(func);
        }

        batch.set_size(10);

        assert_eq!(batch.size, 10);
    }
    #[test]
    fn equal_batch_clone() {
        fn func() {}

        let mut batch = Batch::new();

        for _ in 0..50 {
            batch.add_func(func);
        }


        assert_eq!(batch.length(), 50);
    }

}