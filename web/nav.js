(function() {
  const main = [
    { label: 'Home',        href: '/' },
    { label: 'Press',       href: '/press' },
    { label: 'Web',         href: '/web' },
    { label: 'Clienti',     href: '/clients' },
    { label: 'Giornalisti', href: '/giornalisti' },
    { label: 'Testate',     href: '/testate' },
  ];

  // Pagine da spostare in main quando pronte
  const secondary = [
    { label: 'Intelligence', href: '/intelligence' },
    { label: 'Digest',      href: '/digest' },
    { label: 'Web Digest',  href: '/webdigest' },
    { label: 'AI Analysis', href: '/chat' },
    { label: 'AI Pitch',    href: '/pitch' },
  ];

  const current = window.location.pathname.replace(/\/$/, '') || '/';

  const nav = document.getElementById('topnav');
  if (!nav) return;

  const mkLink = l => {
    const href   = l.href.replace(/\/$/, '') || '/';
    const active = current === href ? ' active' : '';
    return `<a href="${l.href}" class="nav-link${active}">${l.label}</a>`;
  };

  const mainLinks      = main.map(mkLink).join('');
  const secondaryLinks = secondary.map(mkLink).join('');

  nav.innerHTML = `
    <nav>
      <img src="/static/Logo-MAIM_orizzontale.jpg" alt="MAIM" class="logo-img"/>
      <div class="nav-links">
        ${mainLinks}
        <span style="display:inline-block;width:1px;height:20px;background:var(--border2);margin:0 6px;align-self:center;flex-shrink:0;"></span>
        ${secondaryLinks}
      </div>
    </nav>`;
})();