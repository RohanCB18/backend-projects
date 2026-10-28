import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

// Each backend runs on its own port; the UI talks to all of them through
// /api/<n>/... so the browser only ever sees one origin (no CORS fuss).
const backends = { 1: 9101, 2: 9102, 3: 9103, 4: 9104, 5: 9105 }

const proxy = {}
for (const [n, port] of Object.entries(backends)) {
  proxy[`/api/${n}`] = {
    target: `http://127.0.0.1:${port}`,
    changeOrigin: true,
    ws: n === '4', // the collaborative editor uses WebSockets
    rewrite: (path) => path.replace(new RegExp(`^/api/${n}`), ''),
  }
}

export default defineConfig({
  plugins: [react()],
  server: { port: 5173, strictPort: true, proxy },
})
