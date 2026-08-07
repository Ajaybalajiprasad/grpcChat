package main

import (
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net"
	"os"
	"path/filepath"
	"strings"
	"time"

	"grpcchat/internal/chat"

	"github.com/charmbracelet/bubbles/textinput"
	"github.com/charmbracelet/bubbles/viewport"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	"google.golang.org/grpc"
)

var (
	titleStyle  = lipgloss.NewStyle().Foreground(lipgloss.Color("205")).Bold(true).Padding(0, 1).Background(lipgloss.Color("236")).MarginBottom(1)
	infoStyle   = lipgloss.NewStyle().Foreground(lipgloss.Color("241")).Render
	msgStyle    = lipgloss.NewStyle().Foreground(lipgloss.Color("86"))
	meStyle     = lipgloss.NewStyle().Foreground(lipgloss.Color("212")).Bold(true)
	sysStyle    = lipgloss.NewStyle().Foreground(lipgloss.Color("220")).Italic(true)
	borderStyle = lipgloss.NewStyle().Border(lipgloss.RoundedBorder()).BorderForeground(lipgloss.Color("62"))
)

type logMsg string

type model struct {
	node      *chat.Node
	username  string
	listen    string
	viewport  viewport.Model
	textInput textinput.Model
	messages  []string
	ready     bool
}

func initialModel(n *chat.Node, user, listen string) model {
	ti := textinput.New()
	ti.Placeholder = "Type a message or /help..."
	ti.Focus()
	ti.CharLimit = 256
	ti.Width = 50
	ti.PromptStyle = lipgloss.NewStyle().Foreground(lipgloss.Color("205"))
	ti.TextStyle = lipgloss.NewStyle().Foreground(lipgloss.Color("252"))

	return model{
		node:      n,
		username:  user,
		listen:    listen,
		textInput: ti,
		messages:  []string{sysStyle.Render(fmt.Sprintf("Welcome %s! Listening on %s", user, listen))},
	}
}

func (m model) Init() tea.Cmd {
	return tea.Batch(
		textinput.Blink,
		waitForChatMsg(m.node.MsgChan),
		waitForLogMsg(m.node.LogChan),
	)
}

func waitForChatMsg(ch chan *chat.ChatMessage) tea.Cmd {
	return func() tea.Msg {
		return <-ch
	}
}

func waitForLogMsg(ch chan string) tea.Cmd {
	return func() tea.Msg {
		return logMsg(<-ch)
	}
}

func (m model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	var (
		tiCmd tea.Cmd
		vpCmd tea.Cmd
	)

	m.textInput, tiCmd = m.textInput.Update(msg)

	switch msg := msg.(type) {
	case tea.KeyMsg:
		switch msg.Type {
		case tea.KeyCtrlC, tea.KeyEsc:
			return m, tea.Quit

		case tea.KeyEnter:
			v := m.textInput.Value()
			trimmed := strings.TrimSpace(v)
			if trimmed != "" {
				m.textInput.Reset()

				if strings.HasPrefix(trimmed, "/") {
					if strings.HasPrefix(trimmed, "/sendfile ") || strings.HasPrefix(trimmed, "/send ") {
						parts := strings.SplitN(trimmed, " ", 2)
						if len(parts) == 2 {
							filePath := strings.TrimSpace(parts[1])
							data, err := os.ReadFile(filePath)
							if err != nil {
								m.messages = append(m.messages, sysStyle.Render(fmt.Sprintf("Failed to read file: %v", err)))
							} else {
								fileName := filepath.Base(filePath)
								fileMsg := &chat.ChatMessage{
									Id:         fmt.Sprintf("file-%s-%d-%d", m.username, time.Now().UnixNano(), rand.Int63()),
									Username:   m.username,
									Timestamp:  time.Now().Unix(),
									Type:       chat.MessageType_FILE,
									FileName:   fileName,
									FileData:   data,
									FileSize:   int64(len(data)),
									ListenAddr: m.listen,
								}
								m.node.Broadcast(fileMsg, "local-cli")
								m.messages = append(m.messages, meStyle.Render(fmt.Sprintf("[You sent file] %s (%d bytes)", fileName, len(data))))
							}
						}
					} else {
						switch trimmed {
						case "/topology", "/mesh", "/graph":
							topo := m.node.GetTopologyString()
							m.messages = append(m.messages, sysStyle.Render(topo))
						case "/peers", "/list":
							topo := m.node.GetTopologyString()
							m.messages = append(m.messages, sysStyle.Render(topo))
						case "/help":
							helpMsg := "--- Available Commands ---\n" +
								"  /sendfile <path>   : Send file to connected peers\n" +
								"  /topology or /mesh : Display visual network topology graph\n" +
								"  /peers or /list    : Show connected peer nodes\n" +
								"  /help              : Show this help message"
							m.messages = append(m.messages, sysStyle.Render(helpMsg))
						default:
							m.messages = append(m.messages, sysStyle.Render("Unknown command. Type /help for available commands."))
						}
					}
				} else {
					chatMsg := &chat.ChatMessage{
						Id:         fmt.Sprintf("%s-%d-%d", m.username, time.Now().UnixNano(), rand.Int63()),
						Username:   m.username,
						Message:    trimmed,
						Timestamp:  time.Now().Unix(),
						Type:       chat.MessageType_CHAT,
						ListenAddr: m.listen,
					}
					m.node.Broadcast(chatMsg, "local-cli")
					m.messages = append(m.messages, meStyle.Render(fmt.Sprintf("[You] %s", trimmed)))
				}

				m.viewport.SetContent(strings.Join(m.messages, "\n"))
				m.viewport.GotoBottom()
			}
			return m, tea.Batch(tiCmd, vpCmd)
		}

	case tea.WindowSizeMsg:
		headerHeight := lipgloss.Height(m.headerView())
		footerHeight := lipgloss.Height(m.footerView())
		verticalMarginHeight := headerHeight + footerHeight

		if !m.ready {
			m.viewport = viewport.New(msg.Width-4, msg.Height-verticalMarginHeight-2)
			m.viewport.YPosition = headerHeight
			m.viewport.SetContent(strings.Join(m.messages, "\n"))
			m.ready = true
		} else {
			m.viewport.Width = msg.Width - 4
			m.viewport.Height = msg.Height - verticalMarginHeight - 2
		}

	case *chat.ChatMessage:
		var formatted string
		if msg.Type == chat.MessageType_FILE {
			formatted = fmt.Sprintf("[%s sent file] %s (%d bytes) -> saved to ./downloads/%s", msg.Username, msg.FileName, msg.FileSize, filepath.Base(msg.FileName))
		} else {
			formatted = fmt.Sprintf("[%s] %s", msg.Username, msg.Message)
		}
		m.messages = append(m.messages, msgStyle.Render(formatted))
		m.viewport.SetContent(strings.Join(m.messages, "\n"))
		m.viewport.GotoBottom()
		return m, tea.Batch(waitForChatMsg(m.node.MsgChan), tiCmd, vpCmd)


	case logMsg:
		m.messages = append(m.messages, sysStyle.Render(string(msg)))
		m.viewport.SetContent(strings.Join(m.messages, "\n"))
		m.viewport.GotoBottom()
		return m, tea.Batch(waitForLogMsg(m.node.LogChan), tiCmd, vpCmd)
	}

	m.viewport, vpCmd = m.viewport.Update(msg)
	return m, tea.Batch(tiCmd, vpCmd)
}

func (m model) headerView() string {
	title := titleStyle.Render(" gRPC Chat Network ")
	line := strings.Repeat("─", max(0, m.viewport.Width-lipgloss.Width(title)))
	return lipgloss.JoinHorizontal(lipgloss.Center, title, line)
}

func (m model) footerView() string {
	line := strings.Repeat("─", max(0, m.viewport.Width))
	info := infoStyle(fmt.Sprintf("%3.f%%", m.viewport.ScrollPercent()*100))
	line = strings.Repeat("─", max(0, m.viewport.Width-lipgloss.Width(info)))

	footerLine := lipgloss.JoinHorizontal(lipgloss.Center, line, info)

	return lipgloss.JoinVertical(lipgloss.Left, footerLine, m.textInput.View())
}

func (m model) View() string {
	if !m.ready {
		return "\n  Initializing..."
	}
	content := fmt.Sprintf("%s\n%s\n%s", m.headerView(), m.viewport.View(), m.footerView())
	return borderStyle.Render(content)
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func main() {
	username := flag.String("username", "anonymous", "Your username")
	listen := flag.String("listen", ":50051", "Address to listen on")
	peerStr := flag.String("peer", "", "Peer address or comma-separated addresses (e.g. 10.42.0.250:50051)")
	flag.Parse()

	listener, err := net.Listen("tcp", *listen)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	node := chat.NewNode(*username, *listen)
	grpcServer := grpc.NewServer()
	chat.RegisterChatServiceServer(grpcServer, node)

	go func() {
		if err := grpcServer.Serve(listener); err != nil {
			log.Fatalf("failed to serve: %v", err)
		}
	}()

	// Small delay to ensure server started
	time.Sleep(100 * time.Millisecond)

	if *peerStr != "" {
		peers := strings.Split(*peerStr, ",")
		for _, p := range peers {
			p = strings.TrimSpace(p)
			if p != "" {
				node.ConnectToPeer(p)
			}
		}
	}

	m := initialModel(node, *username, *listen)
	p := tea.NewProgram(m, tea.WithAltScreen(), tea.WithMouseCellMotion())

	if _, err := p.Run(); err != nil {
		fmt.Printf("could not run app: %v\n", err)
		os.Exit(1)
	}
}
