import React from 'react';
import ThemeToggle from './ThemeToggle';

interface HeaderProps {
  theme: 'light' | 'dark';
  toggleTheme: () => void;
}

const Header: React.FC<HeaderProps> = ({ theme, toggleTheme }) => {
  return (
    <header className={`${theme === 'light' ? 'bg-gray-200' : 'bg-gray-900'}`}>
        <div className={`${theme === 'light' ? 'text-gray-500' : 'text-gray-400'}`}>
          Sigilcor
        </div>
        <div>
          <ThemeToggle theme={theme} toggleTheme={toggleTheme} />
        </div>
    </header>
  );
};

export default Header;
