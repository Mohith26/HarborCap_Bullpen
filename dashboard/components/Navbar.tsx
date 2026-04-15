'use client';

import Link from 'next/link';
import Image from 'next/image';
import { usePathname } from 'next/navigation';
import { useState } from 'react';

interface NavItem {
  href: string;
  label: string;
  icon: React.ReactNode;
  matchPrefix?: boolean;
}

const navItems: NavItem[] = [
  {
    href: '/',
    label: 'Dashboard',
    icon: (
      <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth={1.6} className="w-[18px] h-[18px]">
        <path strokeLinecap="round" strokeLinejoin="round" d="M3 13l9-9 9 9M5 11v9h4v-6h6v6h4v-9" />
      </svg>
    ),
  },
  {
    href: '/feed',
    label: 'Signal Feed',
    matchPrefix: true,
    icon: (
      <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth={1.6} className="w-[18px] h-[18px]">
        <path strokeLinecap="round" strokeLinejoin="round" d="M4 7h16M4 12h16M4 17h10" />
      </svg>
    ),
  },
  {
    href: '/map',
    label: 'Map',
    icon: (
      <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth={1.6} className="w-[18px] h-[18px]">
        <path strokeLinecap="round" strokeLinejoin="round" d="M9 6.75V15m6-6v8.25m.5 3.5L20.4 18.3c.4-.2.6-.6.6-1V4.8c0-.8-.9-1.4-1.6-1L15.5 5.7c-.3.2-.7.2-1 0L9.5 3.3a1.1 1.1 0 0 0-1 0L3.6 5.7c-.4.2-.6.6-.6 1v12.5c0 .8.9 1.4 1.6 1l3.9-1.9c.3-.2.7-.2 1 0l5 2.5c.3.2.7.2 1 0z" />
      </svg>
    ),
  },
  {
    href: '/submarkets',
    label: 'Submarkets',
    matchPrefix: true,
    icon: (
      <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth={1.6} className="w-[18px] h-[18px]">
        <path strokeLinecap="round" strokeLinejoin="round" d="M3 21V8l9-5 9 5v13M9 21v-7h6v7" />
      </svg>
    ),
  },
  {
    href: '/agents',
    label: 'Agents',
    icon: (
      <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth={1.6} className="w-[18px] h-[18px]">
        <path strokeLinecap="round" strokeLinejoin="round" d="M9 7h6m-7 4h8m-9 4h10M5 21h14a2 2 0 0 0 2-2V5a2 2 0 0 0-2-2H5a2 2 0 0 0-2 2v14a2 2 0 0 0 2 2z" />
      </svg>
    ),
  },
];

function isActive(pathname: string, item: NavItem): boolean {
  if (item.matchPrefix) return pathname === item.href || pathname.startsWith(item.href + '/');
  return pathname === item.href;
}

function NavLinkRow({ item, active, onClick }: { item: NavItem; active: boolean; onClick?: () => void }) {
  return (
    <Link
      href={item.href}
      onClick={onClick}
      className={`group relative flex items-center gap-3 px-3 py-2.5 rounded-lg text-[13px] font-medium transition-colors ${
        active
          ? 'bg-[var(--bg-elevated-hover)] text-text-primary'
          : 'text-text-secondary hover:text-text-primary hover:bg-[var(--bg-elevated)]'
      }`}
    >
      {/* Active indicator bar */}
      <span
        className={`absolute left-0 top-1/2 -translate-y-1/2 w-[3px] rounded-r-full transition-all ${
          active ? 'h-5 bg-[var(--hc-peach)]' : 'h-0 bg-transparent'
        }`}
        aria-hidden
      />
      <span className={`shrink-0 transition-colors ${active ? 'text-[var(--hc-peach)]' : 'text-text-tertiary group-hover:text-text-secondary'}`}>
        {item.icon}
      </span>
      <span className="truncate">{item.label}</span>
    </Link>
  );
}

export default function Navbar() {
  const pathname = usePathname();
  const [mobileOpen, setMobileOpen] = useState(false);

  return (
    <>
      {/* Mobile top bar */}
      <div className="md:hidden fixed top-0 left-0 right-0 z-50 bg-[var(--bg-elevated)]/95 backdrop-blur border-b border-border px-4 h-14 flex items-center justify-between">
        <Link href="/" className="flex items-center gap-2.5">
          <Image
            src="/brand/logo-mark.png"
            alt="HarborCap"
            width={28}
            height={28}
            className="rounded-md"
            priority
          />
          <span className="text-[13px] font-semibold tracking-wide text-text-primary">
            The <span className="text-[var(--hc-peach)]">Bullpen</span>
          </span>
        </Link>
        <button
          onClick={() => setMobileOpen(!mobileOpen)}
          className="text-text-secondary hover:text-text-primary p-1.5 -mr-1.5"
          aria-label="Toggle menu"
        >
          <svg className="w-5 h-5" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={1.8}>
            {mobileOpen ? (
              <path strokeLinecap="round" strokeLinejoin="round" d="M6 18L18 6M6 6l12 12" />
            ) : (
              <path strokeLinecap="round" strokeLinejoin="round" d="M4 7h16M4 12h16M4 17h16" />
            )}
          </svg>
        </button>
      </div>

      {/* Mobile dropdown nav */}
      {mobileOpen && (
        <div className="md:hidden fixed top-14 left-0 right-0 z-40 bg-[var(--bg-elevated)]/98 backdrop-blur border-b border-border">
          <nav className="flex flex-col p-2 gap-0.5">
            {navItems.map((item) => (
              <NavLinkRow
                key={item.href}
                item={item}
                active={isActive(pathname, item)}
                onClick={() => setMobileOpen(false)}
              />
            ))}
          </nav>
        </div>
      )}

      {/* Desktop sidebar */}
      <aside className="hidden md:flex flex-col w-60 shrink-0 bg-[var(--bg-elevated)]/60 backdrop-blur border-r border-border min-h-screen sticky top-0 h-screen">
        {/* Brand */}
        <div className="px-5 pt-6 pb-5 border-b border-divider">
          <Link href="/" className="flex items-center gap-3">
            <Image
              src="/brand/logo-mark.png"
              alt="HarborCap"
              width={36}
              height={36}
              className="rounded-lg shrink-0"
              priority
            />
            <div className="min-w-0">
              <h1 className="text-[15px] font-semibold tracking-tight text-text-primary leading-tight">
                The <span className="text-[var(--hc-peach)]">Bullpen</span>
              </h1>
              <p className="text-[10px] tracking-wider text-text-tertiary mt-0.5 uppercase">
                HarborCap
              </p>
            </div>
          </Link>
        </div>

        {/* Section label */}
        <div className="px-5 pt-5 pb-2">
          <p className="text-[10px] font-semibold uppercase tracking-[0.12em] text-text-tertiary">
            Workspace
          </p>
        </div>

        <nav className="flex flex-col px-2.5 gap-0.5 flex-1">
          {navItems.map((item) => (
            <NavLinkRow key={item.href} item={item} active={isActive(pathname, item)} />
          ))}
        </nav>

        {/* Live indicator */}
        <div className="px-5 py-4 border-t border-divider">
          <div className="flex items-center gap-2">
            <span className="relative flex h-1.5 w-1.5">
              <span className="absolute inset-0 rounded-full bg-[var(--hc-peach)] opacity-60 hc-pulse" />
              <span className="relative rounded-full bg-[var(--hc-peach)] h-1.5 w-1.5" />
            </span>
            <p className="text-[10px] text-text-tertiary uppercase tracking-wider">
              Live · 17 agents
            </p>
          </div>
        </div>
      </aside>
    </>
  );
}
