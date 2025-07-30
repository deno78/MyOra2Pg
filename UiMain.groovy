import groovy.swing.SwingBuilder
import groovy.transform.Field
import javax.swing.*
import java.awt.FlowLayout
import java.awt.BorderLayout
import java.awt.Dimension
import java.awt.Font
import java.awt.event.ActionEvent
import java.awt.Graphics
import java.awt.Color
import javax.swing.JPanel

def swing = new SwingBuilder()
def migrator = new PostgresMigrator()

// db.propertiesから初期値取得
def props = new Properties()
new File('db.properties').withInputStream { stream -> props.load(stream) }
def srcUrlInit = props.getProperty('src.url', '')
def srcUserInit = props.getProperty('src.user', '')
def srcPassInit = props.getProperty('src.password', '')
def srcDriverInit = props.getProperty('src.driver', '') ?: 'oracle.jdbc.driver.OracleDriver'
def pgUrlInit = props.getProperty('postgres.url', '')
def pgUserInit = props.getProperty('postgres.user', '')
def pgPassInit = props.getProperty('postgres.password', '')
def pgDriverInit = props.getProperty('postgres.driver', '') ?: 'org.postgresql.Driver'
def tableListPathInit = props.getProperty('table_list', 'table_list.txt')
def threadCountInit = props.getProperty('thread.count', '4')

@Field initialTableList = []
swing.edt {
frame(title: 'PostgreSQL 移行ツール', size: [800, 700], show: true, defaultCloseOperation: JFrame.EXIT_ON_CLOSE) {
    borderLayout()
    panel(constraints: BorderLayout.NORTH) {
        gridLayout(rows: 6, columns: 1, hgap: 10, vgap: 5)
        // 移行元DB
        panel {
            flowLayout(alignment: FlowLayout.LEFT)
            label(text: '移行元DB:')
            textField(id: 'srcUrlField', columns: 40, text: srcUrlInit)
            button(id: 'srcCheckBtn', text: '接続確認', actionPerformed: { e ->
                def url = swing.srcUrlField.text
                def user = swing.srcUserField.text
                def pass = swing.srcPassField.text
                def driver = swing.srcDriverField.text
                def ok = migrator.checkSrcConnection(url, user, pass, driver)
                JOptionPane.showMessageDialog(null, ok ? '接続元DB接続成功' : '接続元DB接続失敗', '接続確認', ok ? JOptionPane.INFORMATION_MESSAGE : JOptionPane.ERROR_MESSAGE)
            })
        }
        panel {
            flowLayout(alignment: FlowLayout.LEFT)
            label(text: 'User:')
            textField(id: 'srcUserField', columns: 10, text: srcUserInit)
            label(text: 'Pass:')
            passwordField(id: 'srcPassField', columns: 10, text: srcPassInit)
            label(text: 'Driver:')
            textField(id: 'srcDriverField', columns: 20, text: srcDriverInit)
        }
        // 移行先DB
        panel {
            flowLayout(alignment: FlowLayout.LEFT)
            label(text: '移行先DB:')
            textField(id: 'pgUrlField', columns: 40, text: pgUrlInit)
            button(id: 'pgCheckBtn', text: '接続確認', actionPerformed: { e ->
                def url = swing.pgUrlField.text
                def user = swing.pgUserField.text
                def pass = swing.pgPassField.text
                def driver = swing.pgDriverField.text
                def ok = migrator.checkPostgresConnection(url, user, pass, driver)
                JOptionPane.showMessageDialog(null, ok ? '接続先DB接続成功' : '接続先DB接続失敗', '接続確認', ok ? JOptionPane.INFORMATION_MESSAGE : JOptionPane.ERROR_MESSAGE)
            })
        }
        panel {
            flowLayout(alignment: FlowLayout.LEFT)
            label(text: 'User:')
            textField(id: 'pgUserField', columns: 10, text: pgUserInit)
            label(text: 'Pass:')
            passwordField(id: 'pgPassField', columns: 10, text: pgPassInit)
            label(text: 'Driver:')
            textField(id: 'pgDriverField', columns: 20, text: pgDriverInit, editable: false)
        }
        // テーブルリストパス
        panel {
            flowLayout(alignment: FlowLayout.LEFT)
            label(text: 'テーブルリスト:')
            textField(id: 'tableListPathField', columns: 40, text: tableListPathInit)
            button(id: 'openTableListButton', text: '開く', actionPerformed: { e ->
                def chooser = new JFileChooser()
                def currentPath = swing.tableListPathField.text
                if (currentPath) {
                    chooser.setCurrentDirectory(new File(currentPath).getParentFile())
                    chooser.setSelectedFile(new File(currentPath))
                }
                chooser.setDialogTitle('テーブルリストファイルを選択')
                chooser.setFileSelectionMode(JFileChooser.FILES_ONLY)
                int result = chooser.showOpenDialog(null)
                if (result == JFileChooser.APPROVE_OPTION) {
                    swing.tableListPathField.text = chooser.getSelectedFile().getAbsolutePath()
                }
            })
        }
        // 設定保存＋移行開始ボタン
        panel {
            flowLayout(alignment: FlowLayout.RIGHT)
            label(text: '並列実行数:')
            spinner(id: 'threadCountSpinner', model: new SpinnerNumberModel(Integer.parseInt(threadCountInit), 1, 64, 1), preferredSize: new Dimension(60, 28))
            button(id: 'saveConfigButton', text: '設定保存', actionPerformed: { e ->
                props.clear()
                props.setProperty('src.url', swing.srcUrlField.text)
                props.setProperty('src.user', swing.srcUserField.text)
                props.setProperty('src.password', swing.srcPassField.text)
                props.setProperty('src.driver', swing.srcDriverField.text)
                props.setProperty('postgres.url', swing.pgUrlField.text)
                props.setProperty('postgres.user', swing.pgUserField.text)
                props.setProperty('postgres.password', swing.pgPassField.text)
                props.setProperty('postgres.driver', swing.pgDriverField.text)
                props.setProperty('table_list', swing.tableListPathField.text)
                props.setProperty('thread.count', swing.threadCountSpinner.value.toString())
                try {
                    new File('db.properties').withOutputStream { out -> props.store(out, 'DB Settings Saved from UI') }
                    JOptionPane.showMessageDialog(null, '設定を保存しました', '保存', JOptionPane.INFORMATION_MESSAGE)
                } catch (Exception ex) {
                    JOptionPane.showMessageDialog(null, '設定保存失敗: ' + ex.message, '保存', JOptionPane.ERROR_MESSAGE)
                }
            })
            button(id: 'loadTableListButton', text: '読込', actionPerformed: { e ->
                def path = swing.tableListPathField.text
                def tableList = []
                try {
                    tableList = new File(path).readLines().findAll { it.trim() && !it.trim().startsWith('#') }.collect { it.split(',')[0].trim() }
                } catch (Exception ex) {
                    JOptionPane.showMessageDialog(null, 'テーブルリスト読込失敗: ' + ex.message, 'エラー', JOptionPane.ERROR_MESSAGE)
                }
                swing.pendingArea.text = tableList.join('\n')
                initialTableList = tableList
                swing.startButton.enabled = tableList.size() > 0
            })
            button(id: 'startButton', text: '移行開始', enabled: false, actionPerformed: { e ->
                startMigration(swing, migrator)
            })
        }
    }
    panel(constraints: BorderLayout.CENTER) {
        gridLayout(rows: 2, columns: 2, hgap: 10, vgap: 10)
        // 左上: 未処理（高さのみ1.5倍）
        panel {
            borderLayout()
            label(text: '未処理テーブル', constraints: BorderLayout.NORTH, font: new Font('SansSerif', Font.BOLD, 16))
            scrollPane(constraints: BorderLayout.CENTER, preferredSize: new Dimension(10, 330)) {
                textArea(id: 'pendingArea', editable: false, font: new Font('Monospaced', Font.PLAIN, 14))
            }
        }
        // 右上: 完了（高さのみ1.5倍）
        panel {
            borderLayout()
            label(text: '完了テーブル', constraints: BorderLayout.NORTH, font: new Font('SansSerif', Font.BOLD, 16))
            scrollPane(constraints: BorderLayout.CENTER, preferredSize: new Dimension(10, 330)) {
                textArea(id: 'completedArea', editable: false, font: new Font('Monospaced', Font.PLAIN, 14))
            }
        }
        // 左下: 処理中
        panel {
            borderLayout()
            label(text: '処理中テーブル', constraints: BorderLayout.NORTH, font: new Font('SansSerif', Font.BOLD, 16))
            scrollPane(constraints: BorderLayout.CENTER, preferredSize: new Dimension(0, 220)) {
                textArea(id: 'processingArea', editable: false, font: new Font('Monospaced', Font.PLAIN, 14))
            }
        }
        // 右下: エラー
        panel {
            borderLayout()
            label(text: '失敗テーブル', constraints: BorderLayout.NORTH, font: new Font('SansSerif', Font.BOLD, 16), foreground: Color.RED)
            scrollPane(constraints: BorderLayout.CENTER, preferredSize: new Dimension(0, 220)) {
                textArea(id: 'failedArea', editable: false, font: new Font('Monospaced', Font.PLAIN, 14), foreground: Color.RED)
            }
        }
    }
    panel(constraints: BorderLayout.SOUTH) {
        flowLayout()
        label(text: '未処理:')
        textField(id: 'pendingField', columns: 30, editable: false)
        label(text: '処理中:')
        textField(id: 'processingField', columns: 30, editable: false)
        label(text: '完了:')
        textField(id: 'completedField', columns: 30, editable: false)
    }
    panel(constraints: BorderLayout.PAGE_END) {
        flowLayout()
        label(id: 'pendingCountLabel', text: '未処理件数: 0')
        label(id: 'processingCountLabel', text: '処理中件数: 0')
        label(id: 'completedCountLabel', text: '完了件数: 0')
        label(id: 'failedCountLabel', text: '失敗件数: 0', foreground: Color.RED)
    }
}
}



void startMigration(swing, migrator) {
    def pendingArea = swing.pendingArea
    def processingArea = swing.processingArea
    def completedArea = swing.completedArea
    def pendingField = swing.pendingField
    def processingField = swing.processingField
    def completedField = swing.completedField
    def startButton = swing.startButton
    def tableListPath = swing.tableListPathField.text
    def threadCount = swing.threadCountSpinner.value as int

    pendingArea.text = initialTableList.join('\n')
    processingArea.text = ''
    completedArea.text = ''
    pendingField.text = ''
    processingField.text = ''
    completedField.text = ''

    startButton.enabled = false

    // logメソッドをGUIにバインド（今回は未使用）
    migrator.logWriter = null // ファイル出力は無効化（GUIのみ）
    migrator.metaClass.log = { String msg -> }

    // テーブルリストパスとスレッド数を渡す
    migrator.metaClass.TABLE_LIST_FILE = tableListPath
    migrator.threadPoolSize = threadCount

    Thread.start {
        migrator.migrateInMemory()
        // 状態表示（最終結果）
        pendingField.text = migrator.pendingTables.join(', ')
        processingField.text = migrator.processingTables.join(', ')
        completedField.text = migrator.completedResults.collect{ it.tableName }.join(', ')
        updateDisplay(swing, migrator)
        startButton.enabled = true // 終了後に再度有効化
    }

    // 状態監視用タイマー
    def timer = new javax.swing.Timer(500, { e ->
        pendingField.text = migrator.pendingTables.join(', ')
        processingField.text = migrator.processingTables.join(', ')
        completedField.text = migrator.completedResults.collect{ it.tableName }.join(', ')
        updateBarGraph(swing, migrator)
        updateDisplay(swing, migrator)
    })
    timer.start()
}

void updateDisplay(swing, migrator) {
    def pendingArea = swing.pendingArea
    def processingArea = swing.processingArea
    def completedArea = swing.completedArea
    def failedArea = swing.failedArea
    pendingArea.text = migrator.pendingTables.join('\n')
    processingArea.text = migrator.processingTables.join('\n')
    completedArea.text = migrator.completedResults.collect{ it.tableName }.join('\n')
    failedArea.text = migrator.failedResults.collect{ it.tableName + (it.errorMessage ? ' : ' + it.errorMessage : '') }.join('\n')
}

void updateBarGraph(swing, migrator) {
    swing.pendingCountLabel.text = "未処理件数: ${migrator.pendingTables.size()}"
    swing.processingCountLabel.text = "処理中件数: ${migrator.processingTables.size()}"
    swing.completedCountLabel.text = "完了件数: ${migrator.completedResults.size()}"
    swing.failedCountLabel.text = "失敗件数: ${migrator.failedResults.size()}"
}
