/**
 * Google OAuth Login Guard
 * Only authorized Google accounts can access the application
 */

class AuthGuard {
    constructor(config) {
        this.clientId = config.clientId;
        this.onSuccess = config.onSuccess || (() => { });
        this.onError = config.onError || ((error) => console.error(error));

        this.user = null;
        this.isAuthenticated = false;
    }

    /**
     * Initialize Google Sign-In
     */
    init() {
        return new Promise((resolve, reject) => {
            // Load Google Identity Services
            const script = document.createElement('script');
            script.src = 'https://accounts.google.com/gsi/client';
            script.async = true;
            script.defer = true;

            script.onload = () => {
                this.initializeGoogleAuth();
                resolve();
            };

            script.onerror = () => {
                reject(new Error('Failed to load Google Identity Services'));
            };

            document.head.appendChild(script);
        });
    }

    /**
     * Initialize Google OAuth
     */
    initializeGoogleAuth() {
        google.accounts.id.initialize({
            client_id: this.clientId,
            callback: this.handleCredentialResponse.bind(this),
            auto_select: true,
            cancel_on_tap_outside: false
        });

        // Check if already logged in
        this.checkExistingSession();
    }

    /**
     * Check locally stored session
     */
    checkExistingSession() {
        const savedUser = localStorage.getItem('auth_user');
        const savedToken = localStorage.getItem('auth_token');

        if (savedUser && savedToken) {
            try {
                const user = JSON.parse(savedUser);
                // Verify if token is expired
                const tokenData = this.parseJwt(savedToken);
                if (tokenData.exp * 1000 > Date.now()) {
                    this.user = user;
                    this.isAuthenticated = true;
                    this.onSuccess(user);
                    this.hideLoginUI();
                    return;
                }
            } catch (e) {
                console.error('Invalid session data:', e);
            }
        }

        // No valid session, show login UI
        this.showLoginUI();
    }

    /**
     * Handle Google login response
     */
    handleCredentialResponse(response) {
        try {
            const credential = response.credential;
            const userData = this.parseJwt(credential);

            // Save user info and token
            this.user = {
                email: userData.email,
                name: userData.name,
                picture: userData.picture,
                sub: userData.sub
            };
            this.isAuthenticated = true;

            localStorage.setItem('auth_user', JSON.stringify(this.user));
            localStorage.setItem('auth_token', credential);

            this.hideLoginUI();
            this.onSuccess(this.user);

            // Reload page to initialize the application
            location.reload();

        } catch (error) {
            this.onError({
                type: 'AUTH_ERROR',
                message: 'Authentication failed',
                error
            });
        }
    }

    /**
     * Show login UI
     */
    showLoginUI() {
        const loginContainer = document.createElement('div');
        loginContainer.id = 'auth-container';
        loginContainer.innerHTML = `
            <style>
                @media (max-width: 768px) {
                    #auth-background {
                        background-image: url('https://img.cathiefish.art/tradingview/small.jpg') !important;
                    }
                    #auth-card {
                        padding: 30px 20px !important;
                        max-width: 90% !important;
                        margin: 0 20px !important;
                    }
                    #auth-title {
                        font-size: 20px !important;
                    }
                }
                @media (min-width: 769px) and (max-width: 1200px) {
                    #auth-background {
                        background-image: url('https://img.cathiefish.art/tradingview/medium.jpg') !important;
                    }
                }
                @media (min-width: 1201px) {
                    #auth-background {
                        background-image: url('https://img.cathiefish.art/tradingview/large.jpg') !important;
                    }
                }
            </style>
            <div id="auth-background" style="
                position: fixed;
                top: 0;
                left: 0;
                width: 100%;
                height: 100%;
                background-image: url('https://img.cathiefish.art/tradingview/large.jpg');
                background-size: cover;
                background-position: center;
                background-repeat: no-repeat;
                display: flex;
                align-items: center;
                justify-content: center;
                z-index: 10000;
            ">
                <!-- Semi-transparent overlay -->
                <div style="
                    position: absolute;
                    top: 0;
                    left: 0;
                    width: 100%;
                    height: 100%;
                    background: rgba(15, 15, 15, 0.05);
                "></div>

                <!-- Login card -->
                <div id="auth-card" style="
                    position: relative;
                    background: rgba(250, 248, 245, 0.95);
                    padding: 50px 40px;
                    border-radius: 16px;
                    box-shadow: 0 16px 48px rgba(0,0,0,0.6);
                    text-align: center;
                    max-width: 420px;
                    border: 1px solid rgba(255, 255, 255, 0.3);
                    backdrop-filter: blur(10px);
                    -webkit-backdrop-filter: blur(10px);
                ">
                    <h1 id="auth-title" style="
                        color: #2c3e50;
                        margin-bottom: 12px;
                        font-size: 28px;
                        font-weight: 600;
                        font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
                        letter-spacing: -0.5px;
                    ">Tradingview</h1>
                    <p style="
                        color: #5a6c7d;
                        margin-bottom: 35px;
                        font-size: 15px;
                        line-height: 1.5;
                        font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
                    ">Sign in with your authorized Google account to continue</p>
                    <div id="google-signin-button"></div>
                </div>
            </div>
        `;

        document.body.appendChild(loginContainer);

        // Render Google Sign-In button
        google.accounts.id.renderButton(
            document.getElementById('google-signin-button'),
            {
                theme: 'filled_blue',
                size: 'large',
                text: 'signin_with',
                shape: 'rectangular',
                width: 300
            }
        );
    }


    /**
     * Hide login UI
     */
    hideLoginUI() {
        const container = document.getElementById('auth-container');
        if (container) {
            container.remove();
        }
    }

    /**
     * Logout
     */
    logout() {
        this.user = null;
        this.isAuthenticated = false;
        localStorage.removeItem('auth_user');
        localStorage.removeItem('auth_token');

        // Clear Google session
        google.accounts.id.disableAutoSelect();

        // Reload page
        location.reload();
    }

    /**
     * Parse JWT token
     */
    parseJwt(token) {
        const base64Url = token.split('.')[1];
        const base64 = base64Url.replace(/-/g, '+').replace(/_/g, '/');
        const jsonPayload = decodeURIComponent(atob(base64).split('').map(function (c) {
            return '%' + ('00' + c.charCodeAt(0).toString(16)).slice(-2);
        }).join(''));
        return JSON.parse(jsonPayload);
    }

    /**
     * Get current user
     */
    getUser() {
        return this.user;
    }

    /**
     * Check if user is authenticated
     */
    isUserAuthenticated() {
        return this.isAuthenticated;
    }
}

// Export to global scope
window.AuthGuard = AuthGuard;
