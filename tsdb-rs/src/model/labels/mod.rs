use anyhow::{bail, Result};
use std::fmt;

#[derive(Clone, PartialEq, Debug)]
struct Label {
    name: String,
    value: String,
}

impl Label {
    pub fn new(name: String, value: String) -> Self {
        Label { name, value }
    }
}

#[derive(Clone, PartialEq, Debug)]
pub struct Labels(Vec<Label>);

impl Labels {
    pub fn from_string(ss: Vec<&str>) -> Result<Labels> {
        if ss.len() % 2 != 0 {
            bail!("invalid number of strings");
        }

        let mut ret = vec![];
        for i in (0..ss.len()).step_by(2) {
            ret.push(Label {
                name: ss[i].to_string(),
                value: ss[i + 1].to_string(),
            })
        }
        ret.sort_by(|a, b| a.name.cmp(&b.name));

        Ok(Labels(ret))
    }

    fn push(&mut self, label: Label) {
        self.0.push(label)
    }

    fn sort(&mut self) {
        self.0.sort_by(|a, b| a.name.cmp(&b.name));
    }
}

impl fmt::Display for Labels {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let v = self
            .0
            .iter()
            .map(|v| format!("{}=\"{}\"", v.name, v.value)) // TODO: proper quote
            .collect::<Vec<_>>()
            .join(", ");

        write!(f, "{{{}}}", v)
    }
}

#[derive(Clone)]
struct ScratchBuilder {
    add: Labels,
}

impl ScratchBuilder {
    pub fn new() -> Self {
        ScratchBuilder {
            add: Labels(vec![]),
        }
    }

    pub fn add(&mut self, name: String, value: String) {
        self.add.push(Label::new(name, value))
    }

    pub fn sort(&mut self) {
        self.add.sort()
    }

    pub fn labels(&self) -> Labels {
        self.add.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_labels() {
        let ls = Labels::from_string(vec!["t1", "t1", "t2", "t2"]).unwrap();
        assert_eq!("{t1=\"t1\", t2=\"t2\"}", format!("{}", ls));

        let ls = Labels::from_string(vec![]).unwrap();
        assert_eq!("{}", format!("{}", ls));
    }

    #[test]
    fn test_scratch_builder() {
        let mut builder = ScratchBuilder::new();

        let v = vec![
            Label::new("aaa".to_string(), "111".to_string()),
            Label::new("bbb".to_string(), "222".to_string()),
        ];

        for item in v.iter() {
            builder.add(item.name.clone(), item.value.clone());
        }
        builder.sort();

        let ls = Labels::from_string(vec!["aaa", "111", "bbb", "222"]).unwrap();
        assert_eq!(ls, builder.labels());
    }
}
