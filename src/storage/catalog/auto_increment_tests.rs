use super::*;

#[test]
fn test_counter_sequence() {
    let counter = AutoIncrementCounter::new("t", "id", 1);
    assert_eq!(counter.next_value(), 1);
    assert_eq!(counter.next_value(), 2);
    assert_eq!(counter.next_value(), 3);
    assert_eq!(counter.current_value(), 4);
    counter.advance_past(10);
    assert_eq!(counter.next_value(), 11);
    counter.advance_past(5);
    assert_eq!(counter.next_value(), 12);
}

#[test]
fn test_counter_thread_safety() {
    use std::thread;
    let counter = Arc::new(AutoIncrementCounter::new("t", "id", 1));
    let mut handles = vec![];

    for _ in 0..10 {
        let c = Arc::clone(&counter);
        handles.push(thread::spawn(move || c.next_value()));
    }

    let results: Vec<u64> = handles.into_iter().map(|h| h.join().unwrap()).collect();
    let mut sorted = results.clone();
    sorted.sort();

    for (i, item) in sorted.iter().enumerate().take(10) {
        assert_eq!(*item, (i + 1) as u64);
    }
    assert_eq!(counter.current_value(), 11);
}

#[test]
fn test_manager_register() {
    let manager = AutoIncrementManager::new();
    let counter = manager.register("db", "users", "id", 100).unwrap();
    assert_eq!(counter.next_value(), 100);

    let counter2 = manager.get("db", "users").unwrap();
    assert_eq!(counter2.next_value(), 101);
}

#[test]
fn test_manager_unregister() {
    let manager = AutoIncrementManager::new();
    manager.register("db", "temp", "id", 1).unwrap();
    assert!(manager.get("db", "temp").is_some());
    manager.unregister("db", "temp");
    assert!(manager.get("db", "temp").is_none());
}
