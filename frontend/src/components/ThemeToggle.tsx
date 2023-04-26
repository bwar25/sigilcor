import React from 'react';

type ThemeToggleProps = {
  toggleTheme: () => void;
  theme: 'light' | 'dark';
};

const ThemeToggle: React.FC<ThemeToggleProps> = ({ toggleTheme, theme }) => {
  return (
    <button
      className={`theme-toggle ${
        theme === 'light' ? 'theme-toggle-light' : 'theme-toggle-dark'
      }`}
      onClick={toggleTheme}
    >
      <span className="ml-2">{theme === 'light' ? 'Change to Dark Theme' : 'Change to Light Theme'}</span>
    </button>
  );
};

export default ThemeToggle;
