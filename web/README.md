# AutoStock 웹 대시보드 (Vite + React)

## 디렉터리 역할 (헷갈리지 않게)

| 경로 | 역할 |
|------|------|
| **`web/`** (이 폴더) | **유일한 소스** — `src/`만 수정합니다. |
| **`../frontend/`** | **빌드 결과물만** — `npm run build`가 덮어씁니다. 직접 편집하지 마세요. |

예전에 쓰던 **단일 HTML 레거시 SPA**는 제거되었습니다. 기능은 모두 `web/src`의 React 코드로 이전했습니다.

## 개발 서버

```bash
cd web && npm ci && npm run dev
```

`/api`는 `vite.config.ts`의 proxy로 배포된 Hosting에 전달됩니다.

## 프로덕션 빌드 → Firebase Hosting

`vite.config.ts`에서 `build.outDir`이 `../frontend`이므로, 빌드 한 번이면 Hosting용 정적 파일이 준비됩니다.

```bash
cd web && npm run build
cd .. && firebase deploy --only hosting
```

---

아래는 Vite 기본 템플릿 안내(ESLint 확장 등)입니다. 필요할 때만 참고하세요.

<details>
<summary>Vite 템플릿 원문</summary>

This template provides a minimal setup to get React working in Vite with HMR and some ESLint rules.

Currently, two official plugins are available:

- [@vitejs/plugin-react](https://github.com/vitejs/plugin-react/blob/main/packages/plugin-react) uses [Oxc](https://oxc.rs)
- [@vitejs/plugin-react-swc](https://github.com/vitejs/plugin-react-swc/blob/main/packages/plugin-react-swc) uses [SWC](https://swc.rs/)

See also [Vite docs](https://vite.dev/).

</details>
