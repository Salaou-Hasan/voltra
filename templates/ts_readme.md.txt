A NeonDB TypeScript starter with React hooks and a Vite-powered client.

## Layout

```
modules/        hello, set_value, delete_value  (JS reducers)
client/         TypeScript + React app
  src/client.ts   NeonDBClient bootstrap
  src/hooks.tsx   useNeonDBQuery, useNeonDBReducer, NeonDBProvider
  src/example/App.tsx   minimal example
```

## Run the database

```bash
neondb start
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

- Add new reducers in `modules/` — they auto-register on `neondb start`.
- Edit `client/src/example/App.tsx` for your own UI.
- Use `useNeonDBQuery("table WHERE …")` for live data; `useNeonDBReducer("name")`
  to invoke reducers with optimistic updates.
