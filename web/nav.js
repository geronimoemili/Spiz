(function() {
  const links = [
    { label: 'Home',        href: '/' },
    { label: 'Press',       href: '/press' },
    { label: 'Digest',      href: '/webdigest' },
    { label: 'Web',         href: '/web' },
    { label: 'AI Analysis', href: '/chat' },
    { label: 'AI Pitch',    href: '/pitch' },
    { label: 'Clienti',     href: '/clients' },
    { label: 'Giornalisti', href: '/giornalisti' },
    { label: 'Testate',     href: '/testate' },
  ];

  const current = window.location.pathname.replace(/\/$/, '') || '/';

  const nav = document.getElementById('topnav');
  if (!nav) return;

  nav.innerHTML = `
    <nav>
      <img src="/static/Logo-MAIM_orizzontale.jpg" alt="MAIM" class="logo-img"/>
      <div class="nav-links">
        ${links.map(l => {
          const href = l.href.replace(/\/$/, '') || '/';
          const active = (current === href) ? ' active' : '';
          return `<a href="${l.href}" class="nav-link${active}">${l.label}</a>`;
        }).join('')}
      </div>
    </nav>`;
})();