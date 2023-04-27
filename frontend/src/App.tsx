import { useState, useEffect } from 'react';
import ThemeProvider from './contexts/ThemeProvider';
import Header from './components/Header';
import Footer from './components/Footer';


const App = () => {
  const [theme, setTheme] = useState<'light' | 'dark'>('dark');

  useEffect(() => {
    document.body.classList.remove(theme === 'light' ? 'dark' : 'light');
    document.body.classList.add(theme);
  }, [theme]);

  const toggleTheme = () => {
    setTheme(theme === 'light' ? 'dark' : 'light');
  };

  return (
    <ThemeProvider>
      <div>
        <Header theme={theme} toggleTheme={toggleTheme} />
        <Footer theme={theme} />
      </div>
    </ThemeProvider>
  );
};

export default App;