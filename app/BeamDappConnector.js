/**
 * BEAM DApp Connector - Stable Wallet Integration Library
 * (from AllDapps — with mobile injection fix)
 *
 * A robust, production-ready library for connecting BEAM dApps to various
 * BEAM wallet environments (Desktop, Web Extension, Mobile, Headless).
 *
 * Features:
 * - Automatic reconnection with exponential backoff
 * - Connection state management
 * - Event-driven architecture
 * - Promise-based API with timeouts
 * - Heartbeat mechanism for connection monitoring
 * - Memory-safe callback handling
 * - TypeScript-ready JSDoc annotations
 *
 * @version 2.0.0
 * @license MIT
 * @author BEAM Community
 */

// ============================================================================
// CONSTANTS
// ============================================================================

const MIN_AMOUNT = 0.00000001;
const MAX_AMOUNT = 254000000;
const BEAM_DECIMALS = 8;

/** Connection states */
const ConnectionState = {
    DISCONNECTED: 'disconnected',
    CONNECTING: 'connecting',
    CONNECTED: 'connected',
    RECONNECTING: 'reconnecting',
    LOCKED: 'locked',
    ERROR: 'error'
};

/** Wallet environments */
const WalletEnvironment = {
    DESKTOP: 'desktop',      // Qt WebEngine (BEAM Desktop Wallet)
    WEB: 'web',              // Chrome Extension
    MOBILE: 'mobile',        // Android/iOS App
    HEADLESS: 'headless',    // WebAssembly client
    UNKNOWN: 'unknown'
};

/** Default configuration */
const DEFAULT_CONFIG = {
    apiVersion: 'current',
    minApiVersion: '',
    appName: 'BEAM DApp',
    headlessNode: 'eu-node01.masternet.beam.mw:8200',
    connectionTimeout: 30000,      // 30 seconds
    reconnectDelay: 1000,          // Initial reconnect delay
    maxReconnectDelay: 30000,      // Max reconnect delay
    maxReconnectAttempts: 10,      // Max reconnect attempts (0 = infinite)
    heartbeatInterval: 15000,      // Heartbeat every 15 seconds
    callTimeout: 60000,            // API call timeout
    autoReconnect: true,
    showLoader: true,
    debug: false
};

// ============================================================================
// EVENT EMITTER
// ============================================================================

/**
 * Simple event emitter for connection state changes
 */
class EventEmitter {
    constructor() {
        this._events = {};
    }

    on(event, callback) {
        if (!this._events[event]) {
            this._events[event] = [];
        }
        this._events[event].push(callback);
        return () => this.off(event, callback);
    }

    once(event, callback) {
        const wrapper = (...args) => {
            this.off(event, wrapper);
            callback(...args);
        };
        this.on(event, wrapper);
    }

    off(event, callback) {
        if (!this._events[event]) return;
        this._events[event] = this._events[event].filter(cb => cb !== callback);
    }

    emit(event, ...args) {
        if (!this._events[event]) return;
        this._events[event].forEach(cb => {
            try {
                cb(...args);
            } catch (err) {
                console.error(`Event handler error for '${event}':`, err);
            }
        });
    }
}

// ============================================================================
// MAIN CONNECTOR CLASS
// ============================================================================

class BeamDappConnector extends EventEmitter {

    constructor(config = {}) {
        super();

        this.config = { ...DEFAULT_CONFIG, ...config };
        this.state = ConnectionState.DISCONNECTED;
        this.environment = WalletEnvironment.UNKNOWN;
        this.wallet = null;
        this.api = null;
        this.styles = null;

        this._callId = 0;
        this._pendingCalls = new Map();
        this._callTimeouts = new Map();

        this._reconnectAttempts = 0;
        this._reconnectTimer = null;

        this._heartbeatTimer = null;
        this._lastHeartbeat = null;

        this._messageListener = null;

        this._log('BeamDappConnector initialized', this.config);
    }

    // ========================================================================
    // PUBLIC API
    // ========================================================================

    getState() { return this.state; }
    isConnected() { return this.state === ConnectionState.CONNECTED; }
    getEnvironment() { return this.environment; }

    async connect(options = {}) {
        if (this.state === ConnectionState.CONNECTED) {
            this._log('Already connected');
            return true;
        }

        if (this.state === ConnectionState.CONNECTING) {
            this._log('Connection already in progress');
            return this._waitForConnection();
        }

        this._setState(ConnectionState.CONNECTING);
        this._detectEnvironment();

        const headless = options.headless || false;

        try {
            await this._connectToWallet(headless);
            this._setState(ConnectionState.CONNECTED);
            this._startHeartbeat();
            this._reconnectAttempts = 0;

            if (this.config.showLoader) {
                this._hideLoader();
            }

            return true;
        } catch (error) {
            this._log('Connection failed:', error);
            this._setState(ConnectionState.ERROR);

            if (this.config.autoReconnect) {
                this._scheduleReconnect();
            }

            throw error;
        }
    }

    async disconnect() {
        this._stopHeartbeat();
        this._clearReconnectTimer();
        this._cancelAllPendingCalls('Disconnected');
        this._removeMessageListener();

        if (this.environment === WalletEnvironment.HEADLESS && this.wallet?.client) {
            try {
                await this._stopHeadlessWallet();
            } catch (e) {
                this._log('Error stopping headless wallet:', e);
            }
        }

        this.wallet = null;
        this.api = null;
        this._setState(ConnectionState.DISCONNECTED);
    }

    async callApi(method, params = {}, timeout = this.config.callTimeout) {
        if (!this.isConnected()) {
            throw new Error('Not connected to wallet');
        }

        const callId = `call-${++this._callId}`;

        return new Promise((resolve, reject) => {
            const timeoutId = setTimeout(() => {
                this._pendingCalls.delete(callId);
                this._callTimeouts.delete(callId);
                reject(new Error(`API call '${method}' timed out after ${timeout}ms`));
            }, timeout);

            this._callTimeouts.set(callId, timeoutId);
            this._pendingCalls.set(callId, { resolve, reject, method });

            const request = {
                jsonrpc: '2.0',
                id: callId,
                method: method,
                params: params
            };

            this._log('API call:', method, params);
            this._sendRequest(request);
        });
    }

    async invokeContract(args, contractBytes = null, createTx = false) {
        const params = { create_tx: createTx };

        if (args) params.args = args;
        if (contractBytes) params.contract = Array.from(contractBytes);

        const result = await this.callApi('invoke_contract', params);

        // Parse shader output if present
        if (result?.output && typeof result.output === 'string') {
            try {
                const parsed = JSON.parse(result.output);
                if (parsed.error) throw new Error(parsed.error);
                // Preserve raw_data for transaction processing (createTx=true)
                if (result.raw_data) parsed.raw_data = result.raw_data;
                if (result.txid) parsed.txid = result.txid;
                return parsed;
            } catch (e) {
                if (e.message && !e.message.includes('JSON')) throw e;
                return result;
            }
        }

        return result;
    }

    async processInvokeData(data) {
        return this.callApi('process_invoke_data', { data });
    }

    async getWalletStatus() { return this.callApi('wallet_status'); }
    async getAddressList(own = true) { return this.callApi('addr_list', { own }); }
    async getUtxoList(assetId = 0) { return this.callApi('get_utxo', { asset_id: assetId }); }
    async getTxList(filter = {}) { return this.callApi('tx_list', { filter }); }
    async getAssetsList() { return this.callApi('assets_list'); }

    async downloadShader(url) {
        return new Promise((resolve, reject) => {
            const xhr = new XMLHttpRequest();
            xhr.open('GET', url, true);
            xhr.responseType = 'arraybuffer';
            xhr.onload = () => {
                if (xhr.status === 200) {
                    const byteArray = new Uint8Array(xhr.response);
                    if (!byteArray.length) { reject(new Error('Empty shader file')); return; }
                    resolve(byteArray);
                } else {
                    reject(new Error(`Failed to download shader: HTTP ${xhr.status}`));
                }
            };
            xhr.onerror = () => reject(new Error('Network error downloading shader'));
            xhr.ontimeout = () => reject(new Error('Timeout downloading shader'));
            xhr.timeout = 30000;
            xhr.send();
        });
    }

    // ========================================================================
    // ENVIRONMENT DETECTION
    // ========================================================================

    _detectEnvironment() {
        const ua = navigator.userAgent;

        if (/QtWebEngine/i.test(ua)) {
            this.environment = WalletEnvironment.DESKTOP;
        } else if (/android/i.test(ua) || /iPad|iPhone|iPod/.test(ua)) {
            this.environment = WalletEnvironment.MOBILE;
        } else {
            this.environment = WalletEnvironment.WEB;
        }

        this._log('Detected environment:', this.environment);
    }

    static isChrome() {
        const ua = navigator.userAgent;
        const isChrome = ua.match(/chrome|chromium|crios/i);
        return isChrome && ua.indexOf('Edg') === -1;
    }

    static isMobile() {
        const ua = navigator.userAgent;
        return /android/i.test(ua) || /iPad|iPhone|iPod/.test(ua);
    }

    static isDesktop() {
        return /QtWebEngine/i.test(navigator.userAgent);
    }

    // ========================================================================
    // CONNECTION METHODS
    // ========================================================================

    async _connectToWallet(headless) {
        if (this.config.showLoader) {
            this._showLoader(headless);
        }

        switch (this.environment) {
            case WalletEnvironment.DESKTOP:
                return this._connectDesktop();

            case WalletEnvironment.MOBILE:
                return this._connectMobile();

            case WalletEnvironment.WEB:
                if (headless) return this._connectHeadless();
                if (!BeamDappConnector.isChrome()) {
                    this._showChromeRequired();
                    throw new Error('Chrome browser required for Web Wallet');
                }
                return this._connectWebExtension();

            default:
                throw new Error('Unknown wallet environment');
        }
    }

    async _connectDesktop() {
        await this._injectScript('qrc:///qtwebchannel/qwebchannel.js');

        return new Promise((resolve, reject) => {
            const timeout = setTimeout(() => {
                reject(new Error('Desktop wallet connection timeout'));
            }, this.config.connectionTimeout);

            try {
                new QWebChannel(qt.webChannelTransport, (channel) => {
                    clearTimeout(timeout);

                    this.wallet = channel.objects.BEAM;
                    this.api = this.wallet.api;
                    this.styles = this.wallet.style;

                    this.api.callWalletApiResult.connect((json) => {
                        this._handleApiResult(json);
                    });

                    this._log('Connected to Desktop wallet');
                    resolve();
                });
            } catch (err) {
                clearTimeout(timeout);
                reject(new Error(`Desktop connection failed: ${err.message}`));
            }
        });
    }

    /**
     * Connect to Mobile wallet (with injection fix from AllDapps)
     */
    async _connectMobile() {
        return new Promise((resolve, reject) => {
            const timeout = setTimeout(() => {
                reject(new Error('Mobile wallet connection timeout'));
            }, this.config.connectionTimeout);

            try {
                if (!window.BEAM) {
                    clearTimeout(timeout);
                    this._showMobileStoreLinks();
                    reject(new Error('BEAM Mobile wallet not available'));
                    return;
                }

                // Android uses document events, iOS uses callback
                if (/android/i.test(navigator.userAgent)) {
                    document.addEventListener('onCallWalletApiResult', (ev) => {
                        this._handleApiResult(ev.detail);
                    });
                } else {
                    window.BEAM.callWalletApiResult((json) => {
                        this._handleApiResult(json);
                    });
                }

                this.wallet = window.BEAM;
                this.api = window.BEAM;

                clearTimeout(timeout);
                this._log('Connected to Mobile wallet');
                resolve();
            } catch (err) {
                clearTimeout(timeout);
                reject(new Error(`Mobile connection failed: ${err.message}`));
            }
        });
    }

    async _connectWebExtension() {
        return new Promise((resolve, reject) => {
            const timeout = setTimeout(() => {
                this._removeMessageListener();
                reject(new Error('Web wallet connection timeout - is BEAM Web Wallet extension installed?'));
            }, this.config.connectionTimeout);

            this._messageListener = async (ev) => {
                if (ev.origin !== window.origin) return;

                this._log('Message received:', ev.data);

                if (ev.data === 'apiInjected') {
                    clearTimeout(timeout);

                    try {
                        this.wallet = { api: window.BeamApi };
                        this.api = window.BeamApi;

                        await window.BeamApi.callWalletApiResult((json) => {
                            this._handleApiResult(json);
                        });

                        this._log('Connected to Web Extension');
                        resolve();
                    } catch (err) {
                        reject(new Error(`Web extension setup failed: ${err.message}`));
                    }
                }

                if (ev.data === 'rejected' || ev.data === 'apiRejected') {
                    clearTimeout(timeout);
                    reject(new Error('Connection rejected by user'));
                }
            };

            window.addEventListener('message', this._messageListener);

            window.postMessage({
                type: 'create_beam_api',
                apiver: this.config.apiVersion,
                apivermin: this.config.minApiVersion,
                appname: this.config.appName,
                is_reconnect: !!window.BeamApi
            }, window.origin);
        });
    }

    async _connectHeadless() {
        await this._injectScript('wasm-client.js');

        return new Promise(async (resolve, reject) => {
            const timeout = setTimeout(() => {
                reject(new Error('Headless wallet connection timeout'));
            }, this.config.connectionTimeout);

            try {
                const WasmModule = await BeamModule();
                const WasmWalletClient = WasmModule.WasmWalletClient;

                const client = new WasmWalletClient(this.config.headlessNode);
                client.startWallet();

                client.subscribe((response) => {
                    this._log('Headless unexpected response:', response);
                });

                client.setApproveContractInfoHandler((info) => {
                    this._log('Headless contract approval requested:', info);
                });

                const appId = WasmWalletClient.GenerateAppID(
                    this.config.appName,
                    window.location.href
                );

                client.createAppAPI(
                    this.config.apiVersion,
                    this.config.minApiVersion,
                    appId,
                    this.config.appName,
                    (err, api) => {
                        clearTimeout(timeout);

                        if (err) {
                            reject(new Error(`Headless API creation failed: ${err}`));
                            return;
                        }

                        api.setHandler((json) => {
                            this._handleApiResult(json);
                        });

                        this.wallet = {
                            headless: true,
                            module: WasmModule,
                            factory: WasmWalletClient,
                            client,
                            appId,
                            api
                        };
                        this.api = api;
                        this.environment = WalletEnvironment.HEADLESS;

                        this._log('Connected to Headless wallet');
                        resolve();
                    }
                );
            } catch (err) {
                clearTimeout(timeout);
                reject(new Error(`Headless connection failed: ${err.message}`));
            }
        });
    }

    async _stopHeadlessWallet() {
        return new Promise((resolve, reject) => {
            this.wallet.client.stopWallet((data) => {
                const running = this.wallet.client.isRunning();
                this._log('Headless wallet stopped:', data, 'still running:', running);
                running ? reject(new Error('Failed to stop')) : resolve();
            });
        });
    }

    // ========================================================================
    // API RESULT HANDLING
    // ========================================================================

    _handleApiResult(json) {
        let response;

        try {
            response = typeof json === 'string' ? JSON.parse(json) : json;
        } catch (err) {
            this._log('Failed to parse API response:', json);
            this.emit('error', new Error('Invalid API response'));
            return;
        }

        this._log('API response:', response);

        const callId = response.id;
        const pending = this._pendingCalls.get(callId);

        const timeoutId = this._callTimeouts.get(callId);
        if (timeoutId) {
            clearTimeout(timeoutId);
            this._callTimeouts.delete(callId);
        }

        if (response.error?.code === -32021 || response.errcode === -5) {
            this._setState(ConnectionState.LOCKED);
            this.emit('locked');

            if (pending) {
                pending.reject(new Error('Wallet is locked'));
                this._pendingCalls.delete(callId);
            }
            return;
        }

        if (response.is_locked === false) {
            if (this.state === ConnectionState.LOCKED) {
                this._setState(ConnectionState.CONNECTED);
                this.emit('unlocked');
            }
        }

        if (response.error) {
            if (pending) {
                pending.reject(new Error(response.error.message || JSON.stringify(response.error)));
                this._pendingCalls.delete(callId);
            } else {
                this.emit('error', response.error);
            }
            return;
        }

        if (pending) {
            pending.resolve(response.result);
            this._pendingCalls.delete(callId);
        }

        this._lastHeartbeat = Date.now();
    }

    _sendRequest(request) {
        const jsonStr = JSON.stringify(request);

        switch (this.environment) {
            case WalletEnvironment.DESKTOP:
            case WalletEnvironment.MOBILE:
            case WalletEnvironment.HEADLESS:
                this.api.callWalletApi(jsonStr);
                break;

            case WalletEnvironment.WEB:
                this.api.callWalletApi(request.id, request.method, request.params);
                break;

            default:
                throw new Error('Cannot send request: unknown environment');
        }
    }

    // ========================================================================
    // RECONNECTION
    // ========================================================================

    _scheduleReconnect() {
        if (!this.config.autoReconnect) return;

        if (this.config.maxReconnectAttempts > 0 &&
            this._reconnectAttempts >= this.config.maxReconnectAttempts) {
            this._log('Max reconnect attempts reached');
            this._setState(ConnectionState.ERROR);
            this.emit('reconnectFailed');
            return;
        }

        const delay = Math.min(
            this.config.reconnectDelay * Math.pow(2, this._reconnectAttempts),
            this.config.maxReconnectDelay
        );

        this._log(`Scheduling reconnect in ${delay}ms (attempt ${this._reconnectAttempts + 1})`);
        this._setState(ConnectionState.RECONNECTING);

        this._reconnectTimer = setTimeout(async () => {
            this._reconnectAttempts++;

            try {
                await this.connect();
                this.emit('reconnected');
            } catch (err) {
                this._log('Reconnect failed:', err);
                this._scheduleReconnect();
            }
        }, delay);
    }

    _clearReconnectTimer() {
        if (this._reconnectTimer) {
            clearTimeout(this._reconnectTimer);
            this._reconnectTimer = null;
        }
    }

    _waitForConnection() {
        return new Promise((resolve) => {
            const handler = (state) => {
                if (state === ConnectionState.CONNECTED) {
                    this.off('stateChange', handler);
                    resolve(true);
                } else if (state === ConnectionState.ERROR || state === ConnectionState.DISCONNECTED) {
                    this.off('stateChange', handler);
                    resolve(false);
                }
            };
            this.on('stateChange', handler);
        });
    }

    // ========================================================================
    // HEARTBEAT
    // ========================================================================

    _startHeartbeat() {
        this._stopHeartbeat();
        this._lastHeartbeat = Date.now();

        this._heartbeatTimer = setInterval(async () => {
            if (!this.isConnected()) return;

            const timeSinceLastHeartbeat = Date.now() - this._lastHeartbeat;

            if (timeSinceLastHeartbeat > this.config.heartbeatInterval * 2) {
                this._log('Connection may be stale, checking...');

                try {
                    await this.callApi('wallet_status', {}, 5000);
                } catch (err) {
                    this._log('Heartbeat failed:', err);
                    this._setState(ConnectionState.DISCONNECTED);

                    if (this.config.autoReconnect) {
                        this._scheduleReconnect();
                    }
                }
            }
        }, this.config.heartbeatInterval);
    }

    _stopHeartbeat() {
        if (this._heartbeatTimer) {
            clearInterval(this._heartbeatTimer);
            this._heartbeatTimer = null;
        }
    }

    // ========================================================================
    // STATE MANAGEMENT
    // ========================================================================

    _setState(newState) {
        if (this.state === newState) return;

        const oldState = this.state;
        this.state = newState;

        this._log(`State: ${oldState} -> ${newState}`);
        this.emit('stateChange', newState, oldState);
        this.emit(newState);
    }

    _cancelAllPendingCalls(reason) {
        for (const [callId, pending] of this._pendingCalls) {
            const timeoutId = this._callTimeouts.get(callId);
            if (timeoutId) clearTimeout(timeoutId);
            pending.reject(new Error(reason));
        }
        this._pendingCalls.clear();
        this._callTimeouts.clear();
    }

    _removeMessageListener() {
        if (this._messageListener) {
            window.removeEventListener('message', this._messageListener);
            this._messageListener = null;
        }
    }

    // ========================================================================
    // UI HELPERS
    // ========================================================================

    _showLoader(headless) {
        this._hideLoader();

        const loader = document.createElement('div');
        loader.id = 'beam-connector-loader';
        loader.style.cssText = `
            position: fixed; top: 0; left: 0; width: 100%; height: 100%;
            background: rgba(4, 37, 72, 0.95); display: flex;
            align-items: center; justify-content: center; z-index: 99999;
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
        `;

        const content = document.createElement('div');
        content.style.cssText = 'text-align: center; color: #fff; padding: 40px; max-width: 500px;';

        content.innerHTML = `
            <div style="margin-bottom: 24px;">
                <svg width="60" height="60" viewBox="0 0 60 60" fill="none">
                    <circle cx="30" cy="30" r="28" stroke="#25c2a0" stroke-width="4"/>
                    <circle cx="30" cy="30" r="8" fill="#25c2a0">
                        <animate attributeName="opacity" values="1;0.3;1" dur="1.5s" repeatCount="indefinite"/>
                    </circle>
                </svg>
            </div>
            <h2 style="margin: 0 0 12px 0; font-size: 24px; font-weight: 600;">
                Connecting to BEAM Wallet
            </h2>
            <p style="margin: 0 0 24px 0; color: #94a3b8; font-size: 14px;">
                ${headless
                    ? 'Initializing secure connection...'
                    : `Please approve the connection request in your BEAM wallet to use ${this.config.appName}`
                }
            </p>
            <div id="beam-connector-buttons" style="display: flex; gap: 12px; justify-content: center;">
                <button id="beam-retry-btn" style="
                    padding: 12px 24px; border-radius: 50px; border: none;
                    background: rgba(255,255,255,0.1); color: #fff;
                    font-weight: 600; cursor: pointer; transition: all 0.2s;
                ">Try Again</button>
                ${this.environment === WalletEnvironment.WEB ? `
                    <button id="beam-install-btn" style="
                        padding: 12px 24px; border-radius: 50px; border: none;
                        background: #00f6d2; color: #042548;
                        font-weight: 600; cursor: pointer; transition: all 0.2s;
                    ">Install Web Wallet</button>
                ` : ''}
            </div>
        `;

        loader.appendChild(content);
        document.body.appendChild(loader);

        document.getElementById('beam-retry-btn')?.addEventListener('click', () => {
            this._hideLoader();
            this.connect();
        });

        document.getElementById('beam-install-btn')?.addEventListener('click', () => {
            window.open('https://chrome.google.com/webstore/detail/beam-web-wallet/ilhaljfiglknggcoegeknjghdgampffk', '_blank');
        });
    }

    _hideLoader() {
        const loader = document.getElementById('beam-connector-loader');
        if (loader) loader.remove();
    }

    _showChromeRequired() {
        this._hideLoader();
        const overlay = document.createElement('div');
        overlay.id = 'beam-connector-loader';
        overlay.style.cssText = `
            position: fixed; top: 0; left: 0; width: 100%; height: 100%;
            background: linear-gradient(to bottom, #035b8f, #042548);
            display: flex; align-items: center; justify-content: center;
            z-index: 99999; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
        `;
        overlay.innerHTML = `
            <div style="text-align: center; color: #fff;">
                <p style="font-size: 18px; font-weight: bold; margin-bottom: 16px;">Browser Not Supported</p>
                <p style="color: #00f6d2; cursor: pointer;" onclick="window.open('https://www.google.com/chrome/', '_blank')">
                    Download Chrome Browser
                </p>
            </div>
        `;
        document.body.appendChild(overlay);
    }

    _showMobileStoreLinks() {
        this._hideLoader();
        const isAndroid = /android/i.test(navigator.userAgent);
        const storeUrl = isAndroid
            ? 'https://play.google.com/store/apps/details?id=com.mw.beam.beamwallet.mainnet'
            : 'https://apps.apple.com/us/app/beam-privacy-wallet/id1459842353';

        const overlay = document.createElement('div');
        overlay.id = 'beam-connector-loader';
        overlay.style.cssText = `
            position: fixed; top: 0; left: 0; width: 100%; height: 100%;
            background: linear-gradient(to bottom, #035b8f, #042548);
            display: flex; align-items: center; justify-content: center;
            z-index: 99999; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
        `;
        overlay.innerHTML = `
            <div style="text-align: center; color: #fff;">
                <p style="font-size: 20px; margin-bottom: 16px;">BEAM Wallet Required</p>
                <p style="color: #00f6d2; cursor: pointer; font-size: 18px;"
                   onclick="window.open('${storeUrl}', '_blank')">
                    Download BEAM Wallet
                </p>
            </div>
        `;
        document.body.appendChild(overlay);
    }

    // ========================================================================
    // UTILITIES
    // ========================================================================

    async _injectScript(url) {
        return new Promise((resolve, reject) => {
            const script = document.createElement('script');
            script.type = 'text/javascript';
            script.async = true;
            script.src = url;
            script.onload = resolve;
            script.onerror = () => reject(new Error(`Failed to load script: ${url}`));
            document.head.appendChild(script);
        });
    }

    _log(...args) {
        if (this.config.debug) {
            console.log('[BeamConnector]', ...args);
        }
    }

    // ========================================================================
    // STATIC UTILITIES
    // ========================================================================

    static validateAmount(value) {
        const regex = /^-?\d+(\.\d*)?$/;
        const str = String(value);
        if (!str.match(regex)) return false;
        const floatValue = parseFloat(str);
        if (isNaN(floatValue)) return false;
        const afterDot = str.indexOf('.') > 0 ? str.substring(str.indexOf('.') + 1) : '0';
        if (afterDot.length > BEAM_DECIMALS) return false;
        if (floatValue < MIN_AMOUNT || floatValue > MAX_AMOUNT) return false;
        return true;
    }

    static beamToGroth(beam) {
        return Math.round(parseFloat(beam) * Math.pow(10, BEAM_DECIMALS));
    }

    static grothToBeam(groth) {
        return (groth / Math.pow(10, BEAM_DECIMALS)).toFixed(BEAM_DECIMALS);
    }

    static formatAmount(amount, decimals = BEAM_DECIMALS) {
        const num = typeof amount === 'string' ? parseFloat(amount) : amount;
        return num.toLocaleString(undefined, {
            minimumFractionDigits: 0,
            maximumFractionDigits: decimals
        });
    }
}

// ============================================================================
// UMD export for script tag usage (no ES module exports — avoids SyntaxError)
// ============================================================================
if (typeof window !== 'undefined') {
    window.BeamDappConnector = BeamDappConnector;
    window.ConnectionState = ConnectionState;
    window.WalletEnvironment = WalletEnvironment;
}
