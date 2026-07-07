// Simple background Service Worker to enable PWA installation capabilities
const CACHE_NAME = 'matrix-bot-v1';

self.addEventListener('install', (event) => {
    self.skipWaiting();
});

self.addEventListener('fetch', (event) => {
    // Let requests pass straight through to the live dashboard server
    event.respondWith(fetch(event.request));
});