// VoltraBehaviour — Unity MonoBehaviour wrapper for VoltraClient.
//
// Add to any GameObject. It connects on Start, pumps subscription callbacks
// onto the main thread every frame, and exposes the client to your scripts:
//
//   var voltra = GetComponent<VoltraBehaviour>();
//   var result = await voltra.Client.Call("spawn", new object[] { "p1", 0, 0, "warrior" });
//   voltra.Client.Subscribe("players WHERE zone = 'z_0_0'", diff => { ... });

using System;
using UnityEngine;

namespace Voltra
{
    public class VoltraBehaviour : MonoBehaviour
    {
        [Tooltip("Voltra WebSocket URL")]
        public string url = "ws://127.0.0.1:3000";

        [Tooltip("API key (leave empty when auth is disabled)")]
        public string apiKey = "";

        public VoltraClient Client { get; private set; }
        public bool Connected { get; private set; }

        public event Action OnReady;

        async void Start()
        {
            Client = new VoltraClient(url, string.IsNullOrEmpty(apiKey) ? null : apiKey);
            Client.OnDisconnected += () => Connected = false;
            try
            {
                await Client.Connect();
                Connected = true;
                OnReady?.Invoke();
                Debug.Log($"[Voltra] connected to {url}");
            }
            catch (Exception e)
            {
                Debug.LogError($"[Voltra] connect failed: {e.Message}");
            }
        }

        void Update()
        {
            // Subscription + disconnect callbacks run on the main thread here.
            while (Client != null && Client.MainThreadQueue.TryDequeue(out var action))
            {
                try { action(); }
                catch (Exception e) { Debug.LogException(e); }
            }
        }

        void OnDestroy()
        {
            Client?.Dispose();
        }
    }
}
