import { Pool } from 'pg';
import axios, { AxiosError } from 'axios';

const pool = new Pool({
  user: 'postgres',
  host: 'localhost',
  database: 'postgres',
  password: '589710',
  port: 5432,
});

async function fetchNextTask() {
  const client = await pool.connect();
  try {
    const result = await client.query(
      "UPDATE urls SET status = 'PROCESSING' WHERE id = (SELECT id FROM urls WHERE status = 'NEW' LIMIT 1 FOR UPDATE SKIP LOCKED) RETURNING *;"
    );
    return result.rows[0] || null;
  } finally {
    client.release();
  }
}

async function processUrl(task: { id: number; url: string }) {
  try {
    const response = await axios.get(task.url);
    await pool.query("UPDATE urls SET status = 'DONE', http_code = $1 WHERE id = $2", [response.status, task.id]);
  } catch (error) {
    const err = error as AxiosError
    await pool.query("UPDATE urls SET status = 'ERROR', http_code = $1 WHERE id = $2", [err.response?.status || 500, task.id]);
  }
}

async function worker() {
  while (true) {
    const task = await fetchNextTask();
    if (!task) {
      console.log('No tasks available, retrying...');
      await new Promise(res => setTimeout(res, 5000));
      continue;
    }
    await processUrl(task);
  }
}

async function startWorkers(concurrency: number) {
  for (let i = 0; i < concurrency; i++) {
    worker();
  }
}

startWorkers(5); // Запускаем 5 параллельных обработчиков

// SQL для создания таблицы
const createTableQuery = `
  CREATE TABLE IF NOT EXISTS urls (
    id SERIAL PRIMARY KEY,
    url TEXT NOT NULL,
    status TEXT CHECK (status IN ('NEW', 'PROCESSING', 'DONE', 'ERROR')) DEFAULT 'NEW',
    http_code INTEGER
  );
`;

const insertTestDataQuery = `
  INSERT INTO urls (url, status) VALUES
  ('https://google.com', 'NEW'),
  ('https://reddit.com', 'NEW'),
  ('https://github.com', 'NEW')
  ON CONFLICT DO NOTHING;
`;

async function initializeDatabase() {
  await pool.query(createTableQuery);
  await pool.query(insertTestDataQuery);
  console.log('Database initialized with test data');
}

initializeDatabase();

