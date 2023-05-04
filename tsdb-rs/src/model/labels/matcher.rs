use std::string::ToString;

pub(crate) enum Matcher {
    MatchEqual(MustMatch),
    MatchNotEqual(MustMatch),
}

impl Matcher {
    pub(crate) fn new_must_matcher<T: ToString>(name: T, value: T) -> Self {
        Matcher::MatchEqual(MustMatch {
            name: name.to_string(),
            value: value.to_string(),
        })
    }

    pub(crate) fn matches(&self, s: &str) -> bool {
        match self {
            Matcher::MatchEqual(m) => &m.value == s,
            Matcher::MatchNotEqual(m) => &m.value != s,
        }
    }
}

pub(crate) struct MustMatch {
    pub(crate) name: String,
    pub(crate) value: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_matches() {
        let m = Matcher::new_must_matcher("name", "value");
        assert_eq!(true, m.matches("value"));
    }
}
