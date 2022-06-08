use clap::ArgEnum;

#[derive(ArgEnum, Clone, Debug, PartialEq)]
pub enum Type {
    Csv,
    Json,
    Xlsx,
    Parquet,
}
impl TryFrom<&str> for Type {
    type Error = anyhow::Error;
    fn try_from(s: &str) -> Result<Self, Self::Error> {
        let res = Self::from_str(s, true);
        if let Ok(inner) = res {
            Ok(inner)
        } else {
            use anyhow::anyhow;
            Err(anyhow!(res.unwrap_err()))
        }
    }
}

#[cfg(tests)]
mod test {
    use super::*;

    #[test]
    fn test_convert() -> Result<()> {
        assert_eq!(Type::Csv, Type::try_from("csv")?);
        assert_eq!(Type::Csv, Type::try_from("cSv")?);

        assert_eq!(Type::Csv, Type::try_from("Json")?);
        assert_eq!(Type::Csv, Type::try_from("jSon")?);

        assert_eq!(Type::Csv, Type::try_from("Xlsx")?);
        assert_eq!(Type::Csv, Type::try_from("xLsx")?);

        assert_eq!(Type::Csv, Type::try_from("Parquet")?);
        assert_eq!(Type::Csv, Type::try_from("pArquet")?);
        Ok(())
    }
}
