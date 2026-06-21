A Voltra TypeScript starter with React hooks and a Vite-powered client.

## Layout

```
modules/        hello, set_value, delete_value  (JS reducers)
client/         TypeScript + React app
  src/client.ts   VoltraClient bootstrap
  src/hooks.tsx   useVoltraQuery, useVoltraReducer, VoltraProvider
  src/example/App.tsx   minimal example
```

## Run the database

```bash
voltra start
```

## Run the client

```bash
cd client
npm install
npm run dev
```

Then open the printed URL. Calls and subscriptions flow over a WebSocket
to `ws://127.0.0.1:3000`.

## Customize

- Add new reducers in `modules/` — they auto-register on `voltra start`.
- Edit `client/src/example/App.tsx` for your own UI.
- Use `useVoltraQuery("table WHERE …")` for live data; `useVoltraReducer("name")`
  to invoke reducers with optimistic updates.
