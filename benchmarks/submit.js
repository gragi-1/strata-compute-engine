import http from 'k6/http';
import { check } from 'k6';

export const options = { vus: 16, duration: '30s', thresholds: {
  http_req_failed: ['rate<0.01'], http_req_duration: ['p(95)<1000'],
}};
export default function () {
  const response = http.post(`${__ENV.STRATA_API_URL || 'http://localhost:8000'}/jobs`, JSON.stringify({
    name: `load-${__VU}-${__ITER}`, image: 'strata/python-workloads:local',
    command: ['python', '/app/main.py', 'monte-carlo', '--samples', '100000'],
    resources: {cpu: 1, memory_mb: 128}, max_retries: 0, timeout_seconds: 120,
  }), {headers: {'Content-Type': 'application/json'}});
  check(response, {'admitted or explicitly backpressured': r => [201, 429].includes(r.status)});
}
