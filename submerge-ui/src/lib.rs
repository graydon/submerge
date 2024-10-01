use crossterm::{
    event::{self, KeyCode, KeyEventKind},
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
    ExecutableCommand,
};
use ratatui::{
    prelude::{CrosstermBackend, Stylize, Terminal},
    widgets::Paragraph,
};
use std::io::{stdout, Result};

pub fn run_ui() -> Result<()> {
    stdout().execute(EnterAlternateScreen)?;
    enable_raw_mode()?;
    let res = main_loop();
    stdout().execute(LeaveAlternateScreen)?;
    disable_raw_mode()?;
    res
}

fn main_loop() -> Result<()> {
    let mut terminal = Terminal::new(CrosstermBackend::new(stdout()))?;
    terminal.clear()?;
    loop {
        draw_ui(&mut terminal)?;
        match handle_events()? {
            Some(UIEvent::Quit) => break,
            None => (),
        }
    }
    Ok(())
}

fn draw_ui(terminal: &mut Terminal<CrosstermBackend<std::io::Stdout>>) -> Result<()> {
    terminal.draw(|frame| {
        let area = frame.size();
        frame.render_widget(
            Paragraph::new("Submerge (press 'q' to quit)")
                .white()
                .on_blue(),
            area,
        );
    })?;
    Ok(())
}

enum UIEvent {
    Quit,
}

fn handle_events() -> Result<Option<UIEvent>> {
    if event::poll(std::time::Duration::from_millis(16))? {
        if let event::Event::Key(key) = event::read()? {
            if key.kind == KeyEventKind::Press && key.code == KeyCode::Char('q') {
                return Ok(Some(UIEvent::Quit));
            }
        }
    }
    Ok(None)
}
