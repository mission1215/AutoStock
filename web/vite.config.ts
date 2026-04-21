import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'
import tailwindcss from '@tailwindcss/vite'

// https://vite.dev/config/
// 빌드 산출물은 레포 루트의 frontend/ (firebase.json hosting.public) — rsync 불필요
export default defineConfig({
  plugins: [react(), tailwindcss()],
  build: {
    outDir: "../frontend",
    emptyOutDir: true,
  },
  server: {
    proxy: {
      // 로컬 `npm run dev` 시 동일 경로 /api → 배포 호스팅으로 전달
      "/api": {
        target: "https://autostock-kis.web.app",
        changeOrigin: true,
        secure: true,
      },
    },
  },
})
