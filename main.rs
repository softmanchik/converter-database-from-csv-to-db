use csv::ReaderBuilder;
use rusqlite::{Connection, ToSql};
use std::fs::File;
use std::io::{BufRead, BufReader};

fn detect_delimiter(path: &str) -> char {
    let file = File::open(path).expect("Не удалось открыть CSV");
    let mut reader = BufReader::new(file);
    let mut sample = String::new();
    reader.read_line(&mut sample).unwrap();

    let delimiters = [',', ';', '\t', '|'];
    let mut best = (',', 0);
    for &d in &delimiters {
        let c = sample.matches(d).count();
        if c > best.1 {
            best = (d, c);
        }
    }
    best.0
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let csv_path = "yandexeda.csv";
    let db_path = "yandexeda.db";
    let batch_size = 100000usize; // <-- можешь менять

    println!("📦 Конвертация CSV → SQLite (FTS5)");
    let delimiter = detect_delimiter(csv_path);
    println!("→ Определён разделитель: '{}'", delimiter);

    let mut rdr = ReaderBuilder::new()
        .delimiter(delimiter as u8)
        .from_path(csv_path)?;

    let headers = rdr
        .headers()?
        .iter()
        .map(|h| h.trim())
        .filter(|h| !h.is_empty())
        .enumerate()
        .map(|(i, h)| {
            if h.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
                h.to_string()
            } else {
                format!("col{}", i + 1)
            }
        })
        .collect::<Vec<_>>();

    println!("→ Найдено полей: {}", headers.len());

    let conn = Connection::open(db_path)?;
    let columns = headers.join(", ");
    let create_sql = format!(
        "CREATE VIRTUAL TABLE IF NOT EXISTS contacts USING fts5({});",
        columns
    );
    conn.execute(&create_sql, [])?;
    println!("→ Таблица contacts создана (FTS5)");

    let placeholders = vec!["?".to_string(); headers.len()].join(", ");
    let insert_sql = format!(
        "INSERT INTO contacts ({}) VALUES ({});",
        headers.join(", "),
        placeholders
    );

    let mut tx = conn.unchecked_transaction()?;
    let mut count = 0usize;
    let mut batch_count = 0usize;

    for result in rdr.records() {
        match result {
            Ok(record) => {
                let values: Vec<String> = record.iter().map(|v| v.trim().to_string()).collect();
                if values.is_empty() {
                    continue;
                }

                // подгоняем длину под headers
                let limited_values = &values[..std::cmp::min(values.len(), headers.len())];
                let params: Vec<&dyn ToSql> =
                    limited_values.iter().map(|v| v as &dyn ToSql).collect();

                // пробуем вставить; игнорируем ошибки
                let _ = tx.execute(&insert_sql, params.as_slice());

                count += 1;
                batch_count += 1;

                if batch_count >= batch_size {
                    tx.commit()?;
                    tx = conn.unchecked_transaction()?;
                    println!("  → вставлено {} строк", count);
                    batch_count = 0;
                }
            }
            Err(err) => {
                eprintln!("⚠️ Пропущена строка (ошибка CSV): {}", err);
                continue;
            }
        }
    }

    tx.commit()?;
    println!("✅ Импорт завершён: {} строк вставлено", count);

    Ok(())
}
