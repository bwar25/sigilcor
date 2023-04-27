import React from 'react';

interface FooterProps {
  theme: 'light' | 'dark';
}

const Footer: React.FC<FooterProps> = ({ theme }) => {
  return (
    <footer className={`${theme === 'light' ? 'bg-gray-200' : 'bg-gray-900'}`}>
        <div className={`${theme === 'light' ? 'text-gray-500' : 'text-gray-400'}`}>
          © 2023 Sigilcor. <br className="lg:hidden" /> All rights reserved.
        </div>
    </footer>
  );
};

export default Footer;