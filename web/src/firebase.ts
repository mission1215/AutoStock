import { initializeApp } from "firebase/app";
import { getAuth, GoogleAuthProvider } from "firebase/auth";

const firebaseConfig = {
  apiKey: "AIzaSyDHSPhqVfrGxf59Hqps_EQa-5DI1ej1U_M",
  authDomain: "autostock-kis.firebaseapp.com",
  projectId: "autostock-kis",
  storageBucket: "autostock-kis.firebasestorage.app",
  messagingSenderId: "360910097151",
  appId: "1:360910097151:web:db7a185ace1d5673bddaca",
};

export const app = initializeApp(firebaseConfig);
export const auth = getAuth(app);
export const googleProvider = new GoogleAuthProvider();
