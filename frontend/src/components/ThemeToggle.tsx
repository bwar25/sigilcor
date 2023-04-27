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
      <span className="ml-2">{theme === 'light' ? 'Dark Ambience' : 'Light Ambience'}</span>
    </button>
  );
};

export default ThemeToggle;
