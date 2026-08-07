use forensic_webhistory::browsers::{
    chrome_time_to_datetime, prtime_to_datetime, webcache::filetime_to_datetime,
};

#[test]
fn formats_exact_known_values_for_all_three_browser_epochs() {
    assert_eq!(
        chrome_time_to_datetime(13_380_163_200_000_000)
            .unwrap()
            .to_rfc3339(),
        "2025-01-01T00:00:00+00:00"
    );
    assert_eq!(
        prtime_to_datetime(1_609_459_200_000_000)
            .unwrap()
            .to_rfc3339(),
        "2021-01-01T00:00:00+00:00"
    );
    assert_eq!(
        filetime_to_datetime(133_801_632_000_000_000)
            .unwrap()
            .to_rfc3339(),
        "2025-01-01T00:00:00+00:00"
    );
    assert_eq!(
        filetime_to_datetime(1).unwrap().timestamp_subsec_nanos(),
        100
    );
}
