# Canvas FileZapper. Zap yer files.

# Monkey patch the File class.
# This is to work around a bug in gems/attachment_fu/lib/attachment_fu#detect_mimetype.
# During att.make_childless, Canvas will call attachment.uploaded_data = data, data being a File
# instance. Attachment#uploaded_data= will call detect_mimetype with data, but will fail if data
# does not respond to #content_type. So we add the content_type method, using the same code that
# detect_mimetype would use anyways.
class File
  def content_type
    File.mime_type?(self)
  end
end

class FileZapper
  # This class deletes user-uploaded and system-generated files, to free up space on disk. It can be
  # used to comply with your institutional data retention policies, and to remove old cruft.

  # USE WITH CAUTION. Files are DELETED FROM DISK and cannot be retrieved.

  # Attachment records are not removed. The underlying files are deleted, and Canvas' native de-dup
  # behaviour is used replace the file with a placeholder. A new placeholder attachment record will
  # be created and set as the root attachment for all deleted attachments.

  # For some fully disposable files like system-generated reports and exports, the files are deleted
  # altogehter, and not replaced with placeholders.

  # Only tested with local storage. Behaviour with S3 is unclear.

  def initialize(options={})
    defaults = {
      cutoff_deleted:        2.year.ago,
      cutoff_content_export: 2.year.ago, # We consider other exports here
      cutoff_epubs:          2.year.ago,
      cutoff_sis_imports:    1.year.ago,
      placeholder_filename:  'OCADU_file_removed_2019',
    }

    @options = defaults.merge(options)
  end

  ################
  # @WIP
  # To Do:
  #   - Files in account-level groups
  #   - Clear out failed uploads?
  ################

  def delete_errors
    # Scan for errored/pending attachments, make sure they have no file on disk
    # workflow_state: errored, pending_upload, unattached? zipping? to_be_zipped? deleted?
  end
  # /@WIP

  def replace_course_files(term)
    term = verify_term(term)

    att_ids = Attachment.where(
      context:    term.courses,
      file_state: :available
    ).pluck(:id)

    # Get files from course groups
    att_ids.concat Attachment.where(
      context:    Group.where(context: term.courses),
      file_state: 'available'
    ).pluck(:id)

    replace_files(att_ids)
  end

  def replace_submissions(term, also=[:comments, :quizzes])
    # Remove student assignment submissions for the given term. Optionally also delete files
    # attached to submissions comments and quiz submission attachments
    term = verify_term(term)

    # Find ALL submissions with attachments for the given terms
    # Pluck attachment IDs (comma-delimited) and flatten them
    # Seach all versions of the submission
    # This is slow, but it may be as good as it gets
    att_ids = Version.where(
      versionable: Submission.where(
        assignment: Assignment.where(context: term.courses)
      ))
      .map { |version|
        ids = YAML::load(version.yaml)['attachment_ids'] || ""
        ids.split(',')
      }
      .flatten
      .compact

    # Submission comment attachments
    if also.include?(:comments)
      att_ids.concat Attachment
        .where(context: Assignment.where(context: term.courses))
        .where.not(workflow_state: :zipped) # Exclude submission exports
        .pluck(:id)
    end

    # Files attached to quiz submissions
    # Attachments are not versioned, so we don't need to account for versions here
    if also.include?(:quizzes)
      att_ids.concat Attachment.where(
        context: Quizzes::QuizSubmission.where(
          quiz:  Quizzes::Quiz.where(context: term.courses)
        )
      ).pluck(:id)
    end

    replace_files(att_ids)
  end

  def delete_deleted_files
    # Remove files that have been manually deleted. Any file deleted before `cutoff_deleted` will be
    # removed from disk. We don't need to replace these, because they're not referenced anywhere.
    Attachment
      .where(file_state: :deleted)
      .where('deleted_at < ?', @options[:cutoff_deleted])
      .find_each do |att|
        destroy_attachment(att)
      end
  end

  def delete_disposable_files
    # Remove all auto-generated files, incuding:
    # [X] Content exports
    # [X] Submission exports
    # [X] ePub files
    # [X] Content Migrations
    # [X] SIS imports
    # [ ] Reports

    delete_content_exports
    delete_submission_exports
    delete_epub_exports
    delete_content_migrations
    delete_sis_batches
  end

  def delete_content_exports
    ContentExport.where('created_at < ?', @options[:cutoff_content_export]).each do |ce|
      log("Deleting ContextExport #{ce.id}")
      delete_content_export_and_attachment(ce)
    end
  end

  def delete_submission_exports
    Attachment.where(
      context_type:   'Assignment',
      workflow_state: 'zipped',
      display_name:   'submissions.zip' # Not strictly necessary, but possibly helpful for the future
    )
    .where('created_at < ?', @options[:cutoff_content_export])
    .find_each do |att|
      log("Deleting Submission Export #{att.id}")
      att.context.decrement!(:submissions_downloads)
      destroy_attachment(att)
    end
  end

  def delete_epub_exports
    # When an EPub export is created, Canvas will first generate a separate ContentExport with
    # attachment, then convert that file to an EPub. We should be safe to nuke both.

    # If the export has associated files, those are exported as a second attachment, so each
    # EpubExport can have one or two attachments
    epubs = EpubExport.where('created_at < ?', @options[:cutoff_epubs]).preload(:attachments, :content_export)
    content_exports = ContentExport.where(id: epubs.map(&:content_export_id)).preload(:attachment)

    epubs.each do |epub|
      log("Deleting ePub Export #{epub.id}")

      # Delete ePub attachments
      epub.attachments.each do |att|
        destroy_attachment(att)
      end

      delete_content_export_and_attachment(epub.content_export)

      # Delete the DB row, rather than mark as deleted. Canvas doesn't like "deleted" ePub exports,
      # and will list it as "generating..." and not allow the user to re-generate
      epub.destroy
    end
  end

  def delete_content_migrations
    # A content migration has three attachments:
    # - attachment for the ContentExport
    # - two attachments for the migration itself
    # Delete them all

    ContentMigration.where('created_at < ?', @options[:cutoff_content_export])
      .preload(:attachment, :overview_attachment, :exported_attachment)
      .find_each do |migration|
        log("Deleting ContentMigration Export #{migration.id}")

        # We need to remove all rows, because ContentMigrations have no "deleted" state.
        # Delete ContentExport and attachment. Workaround FK violations
        migration.content_export.delete
        destroy_attachment(migration.content_export.attachment)
        destroy_attachment(migration.overview_attachment)
        destroy_attachment(migration.exported_attachment)
        migration.delete
      end
  end

  def delete_sis_batches
    batches = SisBatch.where('created_at < ?', @options[:cutoff_sis_imports])

    Attachment.where(context: batches).find_each do |att|
      destroy_attachment(att)
    end

    batches.find_each do |sis|
      begin
        sis.delete
      rescue ActiveRecord::InvalidForeignKey
        # This import is referenced from an account.
      end
    end
  end

  private

    def replace_files(att_ids)
      # Delete the original file from disk and replace it with a handy placeholder
      # Adapted from Attachment#destroy_content_and_replace and Attachments::GarbageCollector

      att_ids.each_slice(500) do |ids_batch|
        Attachment.where(id: ids_batch).each do |att|
          log("Deleting attachment #{att.id}")

          # Find the appropriate placeholder root attachment
          new_root = is_image?(att) ? root_image : root_pdf

          if att.root_attachment_id
            # Skip files we've already processed
            next if att.root_attachment_id == new_root.id

            # Don't delete content from child items. Just set the new root, and save the old root
            # for later reloading
            old_root = att.root_attachment
          else
            old_root = nil

            # This will copy the file to a child and make it the new root
            att.make_childless

            # Delete original file. DANGER!
            begin
              att.destroy_content
              att.thumbnail&.destroy
            rescue Errno::ENOENT
              # The file was not found. Oh well?
            end
          end

          att.root_attachment = new_root
          [:filename, :md5, :size, :content_type].each do |key|
            att.send("#{key}=", new_root.send(key))
          end

          # Fix file extension, so the file will open properly
          unless File.extname(att.display_name) == new_root.extension
            att.display_name = att.display_name + new_root.extension
          end

          att.save!

          # Make sure to update associations on the old root_attachment
          old_root&.reload
        end
      end
    end

    def destroy_attachment(att)
      # Remove the file from disk and mark the attachment as deleted
      log("Deleting Attachment #{att.id}")
      unless att.root_attachment_id
        begin
          att.make_childless
          att.destroy_content
        rescue Errno::ENOENT
          # File not found. That's OK.
        end
      end
      att.destroy
    end

    def delete_content_export_and_attachment(content_export)
      # ContentExport#destroy is broken: PG throws a FK violation when trying to delete the attachment row
      # So we manually delete the content and destroy, rather than delete the attachment
      content_export.attachment&.tap do |att|
        destroy_attachment(att)
      end

      content_export.workflow_state = 'deleted'
      content_export.save!
    end

    def root_pdf
      @root_pdf ||= Attachment.find_by(
        filename: placeholder_pdf_filename,
        context: Account.default,
        root_attachment_id: nil
      ) || create_root_pdf
    end

    def root_image
      @root_image ||= Attachment.find_by(
        filename: placeholder_image_filename,
        context: Account.default,
        root_attachment_id: nil
      ) || create_root_image
    end

    def create_root_pdf
      file_removed_pdf = File.open Rails.root.join('tmp', 'files', 'file_removed.pdf')

      Attachment.new do |att|
        att.context        = Account.default
        att.filename       = placeholder_pdf_filename
        att.uploaded_data  = file_removed_pdf
        att.content_type   = 'application/pdf'
        att.save
      end
    end

    def create_root_image
      file_removed_image = File.open Rails.root.join('tmp', 'files', 'file_removed.png')

      Attachment.new do |att|
        att.context = Account.default
        att.filename       = placeholder_image_filename
        att.uploaded_data  = file_removed_image
        att.content_type   = 'image/png'
        att.save
      end
    end

    def is_image?(att)
      image_types = %w(image/gif image/jpeg image/pjpeg image/png image/x-png image/bmp)
      image_types.include? att.content_type
    end

    def verify_term(term)
      term.is_a?(EnrollmentTerm) ? term : EnrollmentTerm.find_by(sis_source_id: term)
    end

    def log(message)
      Rails.logger.info {"---#{message}"}
    end

    def placeholder_pdf_filename
      "#{@options[:placeholder_filename]}.pdf"
    end

    def placeholder_image_filename
      "#{@options[:placeholder_filename]}.png"
    end
end
