import { useState, useEffect } from 'react';
import ThemeToggle from './components/ThemeToggle';
import ThemeProvider from './contexts/ThemeProvider';

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
        <ThemeToggle theme={theme} toggleTheme={toggleTheme} />
      </div>
    </ThemeProvider>
  );
};

export default App;
